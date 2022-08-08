#pragma once

namespace Redis {

void EncodeBytes(std::string& b, const std::string& data);
void DecodeBytes(std::string& b, std::string* buf);

}