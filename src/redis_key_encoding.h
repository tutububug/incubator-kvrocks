#pragma once

#include <status.h>

namespace Redis {

void EncodeBytes(std::string* b, const std::string& data);
Status DecodeBytes(const std::string& b, size_t& off, std::string* buf);
void EncodeInt(std::string* b, int64_t v);
Status DecodeInt(const std::string& b, size_t& off, int64_t& out);

}