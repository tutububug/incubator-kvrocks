#include <gtest/gtest.h>

#include "redis_key_encoding.h"
#include "encoding.h"
#include "redis_metadata.h"

void printBytes(const std::string& str) {
    for (auto i = 0; i < str.size(); i++) {
        printf("0x%x ", str.c_str()[i]);
    }
    printf("\n");
}

TEST(KeyEncoding, Int) {
    std::string str;
    int64_t in;
    {
        size_t off = 0;
        in = 0x123456;
        Redis::EncodeInt(&str, in);
        auto out = Redis::DecodeInt(str, off);
        assert(in == out);
    }
    {
        str.clear();
        size_t off = 0;
        in = 0;
        Redis::EncodeInt(&str, in);
        auto out = Redis::DecodeInt(str, off);
        assert(in == out);
    }
    {
        str.clear();
        size_t off = 0;
        in = -1;
        Redis::EncodeInt(&str, in);
        auto out = Redis::DecodeInt(str, off);
        assert(in == out);
    }
}

TEST(KeyEncoding, Bytes) {
    std::string str;
    auto in1 = "hello";
    auto in2 = "world";
    auto in3 = "1234567890abc";
    Redis::EncodeBytes(&str, in1);
    Redis::EncodeBytes(&str, in2);
    Redis::EncodeBytes(&str, in3);
    std::cout << "in: ";
    printBytes(str);

    std::string out1;
    std::string out2;
    std::string out3;
    size_t off = 0;
    std::cout << "out1: ";
    Redis::DecodeBytes(str, off, &out1);
    printBytes(out1);
    std::cout << "out2: ";
    Redis::DecodeBytes(str, off, &out2);
    printBytes(out2);
    std::cout << "out3: ";
    Redis::DecodeBytes(str, off, &out3);
    printBytes(out3);

    assert(in1 == out1);
    assert(in2 == out2);
    assert(in3 == out3);
}

TEST(KeyEncoding, TestNamespaceKey) {
    std::string str;
    int64_t table_id_in = 1;
    auto key_in = std::string("ns_key");
    int64_t cf_code = kColumnFamilyIDData;
    auto slot_id_encoded = false;
    ComposeNamespaceKey(table_id_in, key_in, &str, slot_id_encoded, cf_code);

    int64_t table_id_out;
    std::string key_out;
    ExtractNamespaceKey(str, table_id_out, &key_out, slot_id_encoded);

    assert(table_id_in == table_id_out);
    assert(key_in == key_out);
}

TEST(KeyEncoding, TestInternalKey) {
    std::string hash_key("hash_key");
    std::string field_key("field_key");
    int64_t table_id_in = 1;
    uint64_t version = 2;
    auto slot_id_encoded = false;
    int64_t cf_code = kColumnFamilyIDMetadata;

    std::string ns_key;
    ComposeNamespaceKey(table_id_in, hash_key, &ns_key, slot_id_encoded, cf_code);
    auto inner_key_in = InternalKey(ns_key, field_key, version, slot_id_encoded, cf_code);
    assert(inner_key_in.GetNamespace() == table_id_in);
    assert(inner_key_in.GetCF() == kColumnFamilyIDMetadata);
    assert(inner_key_in.GetKey() == hash_key);
    assert(inner_key_in.GetSubKey().ToString() == field_key);

    std::string in;
    inner_key_in.Encode(&in);
    auto inner_key_out = InternalKey(in, slot_id_encoded);
    assert(inner_key_out.GetNamespace() == inner_key_in.GetNamespace());
    assert(inner_key_out.GetCF() == inner_key_in.GetCF());
    assert(inner_key_out.GetKey() == inner_key_in.GetKey());
    assert(inner_key_out.GetSubKey() == inner_key_in.GetSubKey());
}
