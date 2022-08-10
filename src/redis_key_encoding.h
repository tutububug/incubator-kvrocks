#pragma once

namespace Redis {

void EncodeBytes(std::string* b, const std::string& data);
void DecodeBytes(const std::string& b, size_t& off, std::string* buf);
void EncodeInt(std::string* b, int64_t v);
int64_t DecodeInt(const std::string& b, size_t& off);

}