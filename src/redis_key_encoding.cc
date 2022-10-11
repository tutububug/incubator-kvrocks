// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/util/codec/bytes.go
//

// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <sstream>
#include <exception>

namespace Redis {

void reverseBytes(std::string* b);

static const uint8_t encGroupSize = 8;
static const uint8_t encMarker    = 0xFF;
static const uint8_t encPad       = 0x0;

static auto pads = std::string(encGroupSize, encPad);

// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//  [group1][marker1]...[groupN][markerN]
//  group is 8 bytes slice which is padding with 0.
//  marker is `0xFF - padding 0 count`
// For example:
//   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
void EncodeBytes(std::string* b, const std::string& data) {
    // Allocate more space to avoid unnecessary slice growing.
    // Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
    // that is `(len(data) / 8 + 1) * 9` in our implement.
    auto dLen = data.size();
    for (auto idx = 0; idx <= dLen; idx += encGroupSize) {
        auto remain = dLen - idx;
        auto padCount = 0;
        if (remain >= encGroupSize) {
            b->append(std::string(data, idx, encGroupSize));
        } else {
            padCount = encGroupSize - remain;
            b->append(std::string(data, idx));
            b->append(std::string(pads, 0, padCount));
        }

        auto marker = encMarker - padCount;
        b->push_back(marker);
    }
}

void decodeBytes(const std::string& b_in, size_t& off, std::string* buf, bool reverse) {
    buf->clear();
    while (true) {
        auto b = std::string(b_in, off);
        if (b.size() < encGroupSize+1) {
            throw std::runtime_error("insufficient bytes to decode value");
        }

        auto groupBytes = std::string(b, 0, encGroupSize+1);

        auto group = std::string(groupBytes, 0, encGroupSize);
        auto marker = groupBytes[encGroupSize];

        char padCount;
        if (reverse) {
            padCount = marker;
        } else {
            padCount = encMarker - marker;
        }
        if (padCount > encGroupSize) {
            std::ostringstream ss;
            ss << "invalid marker byte, group bytes: " << groupBytes;
            throw std::runtime_error(ss.str());
        }

        auto realGroupSize = encGroupSize - padCount;
        buf->append(std::string(group, 0, realGroupSize));
        off += encGroupSize+1;

        if (padCount != 0) {
            auto padByte = encPad;
            if (reverse) {
                padByte = encMarker;
            }
            // Check validity of padding bytes.
            for (auto it = group.begin()+realGroupSize; it != group.end(); it++) {
                if (*it != padByte) {
                    std::ostringstream ss;
                    ss << "invalid padding byte, group bytes: " << groupBytes;
                    throw std::runtime_error(ss.str());
                }
            }
            break;
        }
    }
    if (reverse) {
        reverseBytes(buf);
    }
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
// `buf` is used to buffer data to avoid the cost of makeslice in decodeBytes when DecodeBytes is called by Decoder.DecodeOne.
void DecodeBytes(const std::string& b, size_t& off, std::string* buf) {
    return decodeBytes(b, off, buf, false);
}

void safeReverseBytes(std::string* b) {
    for (auto it = b->begin(); it != b->end(); it++) {
        *it = ~(*it);
    }
}

void reverseBytes(std::string* b) {
    safeReverseBytes(b);
}

static const uint64_t signMask = 0x8000000000000000;

// EncodeIntToCmpUint make int v to comparable uint type
uint64_t EncodeIntToCmpUint(int64_t v) {
    return static_cast<uint64_t>(v) ^ signMask;
}

// DecodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
int64_t DecodeCmpUintToInt(uint64_t u) {
    return static_cast<int64_t>(u ^ signMask);
}

uint64_t bigEndianUint64(const char* b) {
    return static_cast<uint64_t>(b[7]) |
    static_cast<uint64_t>(b[6])<<8 |
    static_cast<uint64_t>(b[5])<<16 |
    static_cast<uint64_t>(b[4])<<24 |
    static_cast<uint64_t>(b[3])<<32 |
    static_cast<uint64_t>(b[2])<<40 |
    static_cast<uint64_t>(b[1])<<48 |
    static_cast<uint64_t>(b[0])<<56;
}

void bigEndianPutUint64(char* b, uint64_t v) {
    b[0] = static_cast<char>(v >> 56);
    b[1] = static_cast<char>(v >> 48);
    b[2] = static_cast<char>(v >> 40);
    b[3] = static_cast<char>(v >> 32);
    b[4] = static_cast<char>(v >> 24);
    b[5] = static_cast<char>(v >> 16);
    b[6] = static_cast<char>(v >> 8);
    b[7] = static_cast<char>(v);
}

// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
void EncodeInt(std::string* b, int64_t v) {
    char data[8] = {0,};
    auto u = EncodeIntToCmpUint(v);
    bigEndianPutUint64(data, u);
    b->append(data, 8);
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
int64_t DecodeInt(const std::string& b, size_t& off) {
    if (b.size() < 8) {
        throw std::runtime_error("insufficient bytes to decode value");
    }

    auto u = bigEndianUint64(std::string(b, off, 8).c_str());
    auto v = DecodeCmpUintToInt(u);
    off += 8;
    return v;
}

} // namespace Redis
