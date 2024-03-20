/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

/* Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis_hyperloglog.h"

#include <math.h>
#include <stdint.h>

#include "db_util.h"

namespace redis {

rocksdb::Status Hyperloglog::GetMetadata(const Slice &ns_key, HyperloglogMetadata *metadata) {
  return Database::GetMetadata({kRedisHyperloglog}, ns_key, metadata);
}

/* the max 0 pattern counter of the subset the element belongs to is incremented if needed */
rocksdb::Status Hyperloglog::Add(const Slice &user_key, const std::vector<Slice> &elements, uint64_t *ret) {
  *ret = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HyperloglogMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperloglog);
  batch->PutLogData(log_data.Encode());
  if (s.IsNotFound()) {
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }

  Bitmap::SegmentCacheStore cache(storage_, metadata_cf_handle_, ns_key, metadata);
  for (const auto &element : elements) {
    long register_index = 0;
    uint8_t count = hllPatLen((unsigned char *)element.data(), element.size(), &register_index);
    uint32_t segment_index = register_index / kHyperLogLogRegisterCountPerSegment;
    uint32_t register_index_in_segment = register_index % kHyperLogLogRegisterCountPerSegment;

    std::string *segment = nullptr;
    // get segment
    auto s = cache.GetMut(segment_index, &segment);
    if (!s.ok()) return s;
    // segment key not found
    if (segment->size() == 0) {
      std::string seg(kHyperLogLogRegisterBytesPerSegment, 0);
      cache.Set(segment_index, seg);
      cache.GetMut(segment_index, &segment);
    }

    ArrayBitfieldBitmap bitfield(register_index_in_segment);
    // write old_count to bitfield
    auto sts = bitfield.Set(register_index_in_segment, 1, reinterpret_cast<const uint8_t *>(segment->data()+register_index_in_segment));
    if (!sts) return rocksdb::Status::InvalidArgument(sts.Msg());

    uint64_t old_count = 0;
    auto enc = BitfieldEncoding::Create(BitfieldEncoding::Type::kUnsigned, kHyperLogLogBits).GetValue();
    // get old_count as integer
    s = GetBitfieldInteger(bitfield, register_index_in_segment * kHyperLogLogBits, enc, &old_count);
    if (!s.ok()) return s;

    if (count > old_count) {
      // write count to bitfield
      auto sts = bitfield.SetBitfield(register_index_in_segment * kHyperLogLogBits, enc.Bits(), count);
      if (!sts.IsOK()) return rocksdb::Status::InvalidArgument(sts.Msg());
      // write bitfield to segment
      sts = bitfield.Get(register_index_in_segment, 1, reinterpret_cast<uint8_t *>(segment->data()+register_index_in_segment));
      if (!sts.IsOK()) return rocksdb::Status::InvalidArgument(sts.Msg());
      *ret = 1;
    }
  }
  cache.BatchForFlush(batch);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Hyperloglog::Count(const Slice &user_key, uint64_t *ret) {
  *ret = 0;
  std::vector<uint8_t> counts(kHyperLogLogRegisterBytes);
  auto s = getRegisters(user_key, &counts);
  if (!s.ok()) return s;
  auto sts = hllCount(ret, counts);
  if (!sts.IsOK()) return rocksdb::Status::InvalidArgument(sts.Msg());
  return rocksdb::Status::OK();
}

rocksdb::Status Hyperloglog::Merge(const std::vector<Slice> &user_keys) {
  std::vector<uint8_t> max(kHyperLogLogRegisterBytes);
  for (const auto &user_key : user_keys) {
    std::vector<uint8_t> counts(kHyperLogLogRegisterBytes);
    auto s = getRegisters(user_key, &counts);
    if (!s.ok()) return s;
    auto sts = hllMerge(&max[0], counts);
    if (!sts.IsOK()) return rocksdb::Status::InvalidArgument(sts.Msg());
  }

  std::string ns_key = AppendNamespacePrefix(user_keys[0]);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  HyperloglogMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperloglog);
  batch->PutLogData(log_data.Encode());
  if (s.IsNotFound()) {
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }

  Bitmap::SegmentCacheStore cache(storage_, metadata_cf_handle_, ns_key, metadata);
  for (uint32_t i = 0; i < kHyperLogLogRegisterCount; i++) {
    std::string registers(max.begin() + i * kHyperLogLogRegisterBytesPerSegment,
                          max.begin() + (i + 1) * kHyperLogLogRegisterBytesPerSegment);
    std::string *segment = nullptr;
    auto s = cache.GetMut(i, &segment);
    if (!s.ok()) return s;
    *segment = registers;
  }
  cache.BatchForFlush(batch);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

Status hllDenseGetRegister(uint8_t *count, uint8_t *registers, int index) {
  ArrayBitfieldBitmap bitfield(index);
  auto s = bitfield.Set(index, 1, registers+index);
  if (!s.IsOK()) return s;
  return bitfield.Get(index, 1, count);
}

/* ========================= HyperLogLog algorithm  ========================= */

/* Our hash function is MurmurHash2, 64 bit version.
 * It was modified for Redis in order to provide the same result in
 * big and little endian archs (endian neutral). */
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = seed ^ (len * m);
  const uint8_t *data = (const uint8_t *)key;
  const uint8_t *end = data + (len - (len & 7));

  while (data != end) {
    uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
#ifdef USE_ALIGNED_ACCESS
    memcpy(&k, data, sizeof(uint64_t));
#else
    k = *((uint64_t *)data);
#endif
#else
    k = (uint64_t)data[0];
    k |= (uint64_t)data[1] << 8;
    k |= (uint64_t)data[2] << 16;
    k |= (uint64_t)data[3] << 24;
    k |= (uint64_t)data[4] << 32;
    k |= (uint64_t)data[5] << 40;
    k |= (uint64_t)data[6] << 48;
    k |= (uint64_t)data[7] << 56;
#endif

    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    data += 8;
  }

  switch (len & 7) {
    case 7:
      h ^= (uint64_t)data[6] << 48; /* fall-thru */
    case 6:
      h ^= (uint64_t)data[5] << 40; /* fall-thru */
    case 5:
      h ^= (uint64_t)data[4] << 32; /* fall-thru */
    case 4:
      h ^= (uint64_t)data[3] << 24; /* fall-thru */
    case 3:
      h ^= (uint64_t)data[2] << 16; /* fall-thru */
    case 2:
      h ^= (uint64_t)data[1] << 8; /* fall-thru */
    case 1:
      h ^= (uint64_t)data[0];
      h *= m; /* fall-thru */
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

/* Given a string element to add to the HyperLogLog, returns the length
 * of the pattern 000..1 of the element hash. As a side effect 'regp' is
 * set to the register index this element hashes to. */
int Hyperloglog::hllPatLen(unsigned char *ele, size_t elesize, long *regp) {
  uint64_t hash, bit, index;
  int count;

  /* Count the number of zeroes starting from bit kHyperLogLogRegisterCount
   * (that is a power of two corresponding to the first bit we don't use
   * as index). The max run can be 64-P+1 = Q+1 bits.
   *
   * Note that the final "1" ending the sequence of zeroes must be
   * included in the count, so if we find "001" the count is 3, and
   * the smallest count possible is no zeroes at all, just a 1 bit
   * at the first position, that is a count of 1.
   *
   * This may sound like inefficient, but actually in the average case
   * there are high probabilities to find a 1 after a few iterations. */
  hash = MurmurHash64A(ele, elesize, 0xadc83b19ULL);
  index = hash & kHyperLogLogRegisterCountMask;      /* Register index. */
  hash >>= kHyperLogLogRegisterCountPow;                 /* Remove bits used to address the register. */
  hash |= ((uint64_t)1 << kHyperLogLogHashBitCount); /* Make sure the loop terminates
                                     and count will be <= Q+1. */
  bit = 1;
  count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
  while ((hash & bit) == 0) {
    count++;
    bit <<= 1;
  }
  *regp = (int)index;
  return count;
}

/* Compute the register histogram in the dense representation. */
Status hllDenseRegHisto(uint8_t *registers, int *reghisto) {
  for (uint32_t j = 0; j < kHyperLogLogRegisterCount; j++) {
    uint8_t reg;
    auto s = hllDenseGetRegister(&reg, registers, j);
    if (!s.IsOK()) return s;
    reghisto[reg]++;
  }
  return Status::OK();
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level hllDenseRegHisto() and hllSparseRegHisto()
 * functions as helpers to compute histogram of register values part of the
 * computation, which is representation-specific, while all the rest is common. */

/* Helper function sigma as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllSigma(double x) {
  if (x == 1.) return INFINITY;
  double zPrime;
  double y = 1;
  double z = x;
  do {
    x *= x;
    zPrime = z;
    z += x * y;
    y += y;
  } while (zPrime != z);
  return z;
}

/* Helper function tau as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllTau(double x) {
  if (x == 0. || x == 1.) return 0.;
  double zPrime;
  double y = 1.0;
  double z = 1 - x;
  do {
    x = sqrt(x);
    zPrime = z;
    y *= 0.5;
    z -= pow(1 - x, 2) * y;
  } while (zPrime != z);
  return z / 3;
}

/* Return the approximated cardinality of the set based on the harmonic
 * mean of the registers values. 'hdr' points to the start of the SDS
 * representing the String object holding the HLL representation.
 *
 * If the sparse representation of the HLL object is not valid, the integer
 * pointed by 'invalid' is set to non-zero, otherwise it is left untouched.
 *
 * hllCount() supports a special internal-only encoding of HLL_RAW, that
 * is, hdr->registers will point to an uint8_t array of kHyperLogLogRegisterCount element.
 * This is useful in order to speedup PFCOUNT when called against multiple
 * keys (no need to work with 6-bit integers encoding). */
Status Hyperloglog::hllCount(uint64_t *ret, const std::vector<uint8_t> &counts) {
  double m = kHyperLogLogRegisterCount;
  double E;
  int j;
  /* Note that reghisto size could be just kHyperLogLogHashBitCount+2, because kHyperLogLogHashBitCount+1 is
   * the maximum frequency of the "000...1" sequence the hash function is
   * able to return. However it is slow to check for sanity of the
   * input: instead we history array at a safe size: overflows will
   * just write data to wrong, but correctly allocated, places. */
  int reghisto[64] = {0};

  /* Compute register histogram */
  auto sts = hllDenseRegHisto((uint8_t *)(&counts[0]), reghisto);
  if (!sts.IsOK()) return sts;

  /* Estimate cardinality from register histogram. See:
   * "New cardinality estimation algorithms for HyperLogLog sketches"
   * Otmar Ertl, arXiv:1702.01284 */
  double z = m * hllTau((m - reghisto[kHyperLogLogHashBitCount + 1]) / (double)m);
  for (j = kHyperLogLogHashBitCount; j >= 1; --j) {
    z += reghisto[j];
    z *= 0.5;
  }
  z += m * hllSigma(reghisto[0] / (double)m);
  E = llroundl(kHyperLogLogAlphaInf * m * m / z);

  *ret = (uint64_t)E;
  return Status::OK();
}

/* Merge by computing MAX(registers[i],hll[i]) the HyperLogLog 'hll'
 * with an array of uint8_t kHyperLogLogRegisterCount registers pointed by 'max'.
 *
 * The hll object must be already validated via isHLLObjectOrReply()
 * or in some other way.
 *
 * If the HyperLogLog is sparse and is found to be invalid, C_ERR
 * is returned, otherwise the function always succeeds. */
Status Hyperloglog::hllMerge(uint8_t *max, const std::vector<uint8_t> &counts) {
  uint8_t val;
  auto registers = (uint8_t *)(&counts[0]);

  for (uint32_t i = 0; i < kHyperLogLogRegisterCount; i++) {
    auto s = hllDenseGetRegister(&val, registers, i);
    if (!s.IsOK()) return s;
    if (val > max[i]) max[i] = val;
  }
  return Status::OK();
}

rocksdb::Status Hyperloglog::getRegisters(const Slice &user_key, std::vector<uint8_t> *counts) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  HyperloglogMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());

    int register_index = std::stoi(ikey.GetSubKey().ToString());
    auto val = iter->value().ToString();
    // TODO assert the value size must be kHyperLogLogRegisterBytesPerSegment
    std::copy(val.begin(), val.end(), counts->data() + register_index);
  }
  return rocksdb::Status::OK();
}

}  // namespace redis