#include <gtest/gtest.h>

#include "redis_request.h"

void parse_assert(const std::vector<Redis::CommandTokens>& actual,
                  std::vector<Redis::CommandTokens>& expect) {
  assert(actual.size() == expect.size());
  for (auto i = 0; i < expect.size(); i++) {
    assert(actual[i].size() == expect[i].size());
    for (auto j = 0; j < expect[i].size(); j++) {
      assert(actual[i][j] == expect[i][j]);
    }
  }
}

TEST(RedisRequest, ParseSingleCommand) {
  std::string req_str = "*3\r\n$3\r\nset\r\n$1\r\na\r\n$4\r\n1234\r\n";
  Redis::Request req;
  auto s = req.Tokenize(req_str);
  assert(s.IsOK());
  std::vector<Redis::CommandTokens> expect = {
      {"set", "a", "1234"},
  };
  parse_assert(req.GetCommands(), expect);
}

TEST(RedisRequest, ParseCommandArray) {
  std::string req_str = "*3\r\n$3\r\nset\r\n$1\r\na\r\n$4\r\n1234\r\n*3\r\n$3\r\nget\r\n$1\r\nb\r\n$4\r\n5678\r\n";
  Redis::Request req;
  auto s = req.Tokenize(req_str);
  assert(s.IsOK());
  std::vector<Redis::CommandTokens> expect = {
      {"set", "a", "1234"},
      {"get", "b", "5678"},
  };
  parse_assert(req.GetCommands(), expect);
}

TEST(RedisRequest, ParseInlineCommand) {
  std::string req_str = "set hello world\r\nset\tabc\t123\r\n";
  Redis::Request req;
  auto s = req.Tokenize(req_str);
  assert(s.IsOK());
  std::vector<Redis::CommandTokens> expect = {
      {"set", "hello", "world"},
      {"set", "abc", "123"},
  };
  parse_assert(req.GetCommands(), expect);
}
