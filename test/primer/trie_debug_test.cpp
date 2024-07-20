#include <fmt/format.h>
#include <zipfian_int_distribution.h>
#include <bitset>
#include <functional>
#include <numeric>
#include <optional>
#include <random>
#include <thread>  // NOLINT

#include "common/exception.h"
#include "gtest/gtest.h"
#include "primer/trie.h"
#include "primer/trie_answer.h"
#include "trie_debug_answer.h"  // NOLINT

namespace bustub {

int GetChildrenCount(const std::shared_ptr<const TrieNode> &node) {
  if (!node) {
    return 0;
  }
  return node->children_.size();
}

std::shared_ptr<const TrieNode> GetNodeWithPrefix(const std::shared_ptr<const TrieNode> &root,
                                                  const std::string &prefix) {
  auto cur = root;
  for (char c : prefix) {
    if (!cur) {
      return nullptr;
    }
    auto it = cur->children_.find(c);
    if (it == cur->children_.end()) {
      return nullptr;
    }
    cur = it->second;
  }
  return cur;
}

TEST(TrieDebugger, TestCase) {
  std::mt19937_64 gen(23333);
  zipfian_int_distribution<uint32_t> dis(0, 1000);

  auto trie = Trie();
  for (uint32_t i = 0; i < 100; i++) {
    std::string key = fmt::format("{}", dis(gen));
    auto value = dis(gen);
    switch (i) {
      // Test the first 3 values from the random generator.
      case 0:
        ASSERT_EQ(value, 128) << "Random generator not portable, please post on Piazza for help.";
        break;
      case 1:
        ASSERT_EQ(value, 16) << "Random generator not portable, please post on Piazza for help.";
        break;
      case 2:
        ASSERT_EQ(value, 41) << "Random generator not portable, please post on Piazza for help.";
        break;
      default:
        break;
    }
    trie = trie.Put<uint32_t>(key, value);
  }

  // Put a breakpoint here.
  // std::cout << "root children count " << GetChildrenCount(trie.GetRoot()) << std::endl;
  // std::cout << "node of prefix `9` children "
  //           << (GetNodeWithPrefix(trie.GetRoot(), "9") ? GetNodeWithPrefix(trie.GetRoot(), "9")->children_.size() :
  //           0)
  //           << std::endl;
  // std::cout << "value for 969 " << (trie.Get<uint32_t>("969") ? *trie.Get<uint32_t>("969") : 0) << std::endl;

  // (1) How many children nodes are there on the root?
  // Replace `CASE_1_YOUR_ANSWER` in `trie_answer.h` with the correct answer.
  if (CASE_1_YOUR_ANSWER != Case1CorrectAnswer()) {
    ASSERT_TRUE(false) << "case 1 not correct";
  }

  // (2) How many children nodes are there on the node of prefix `9`?
  // Replace `CASE_2_YOUR_ANSWER` in `trie_answer.h` with the correct answer.
  if (CASE_2_YOUR_ANSWER != Case2CorrectAnswer()) {
    ASSERT_TRUE(false) << "case 2 not correct";
  }

  // (3) What's the value for `969`?
  // Replace `CASE_3_YOUR_ANSWER` in `trie_answer.h` with the correct answer.
  if (CASE_3_YOUR_ANSWER != Case3CorrectAnswer()) {
    ASSERT_TRUE(false) << "case 3 not correct";
  }
}

}  // namespace bustub
