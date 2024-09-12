//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
struct UnmatchedTuple {
  Tuple tuple_;
  bool matched_ = false;  // 默认未匹配
};

class JoinHashTable {
 public:
  using JoinKey = std::vector<Value>;
  using JoinValue = std::vector<Tuple>;

  JoinHashTable() = default;

  // 插入操作
  void Insert(const JoinKey &key, const Tuple &value) { ht_[key].push_back(value); }

  // 查找操作
  auto Find(const JoinKey &key) -> std::vector<Tuple> * {
    if (ht_.count(key) > 0) {
      return &ht_[key];
    }
    return nullptr;
  }

  // 清除操作
  void Clear() { ht_.clear(); }

 private:
  struct JoinKeyHash {
    auto operator()(const JoinKey &key) const -> std::size_t {
      std::size_t seed = key.size();
      for (const auto &val : key) {
        seed ^= HashUtil::HashValue(&val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      }
      return seed;
    }
  };

  struct JoinKeyEqual {
    auto operator()(const JoinKey &lhs, const JoinKey &rhs) const -> bool {
      if (lhs.size() != rhs.size()) {
        return false;
      }
      for (size_t i = 0; i < lhs.size(); i++) {
        if (lhs[i].CompareEquals(rhs[i]) != CmpBool::CmpTrue) {
          return false;
        }
      }
      return true;
    }
  };

  std::unordered_map<JoinKey, JoinValue, JoinKeyHash, JoinKeyEqual> ht_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** Left and right child executors */
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  /** Hash table to store tuples from the left child */
  JoinHashTable hash_table_;

  std::vector<UnmatchedTuple> left_tuples_unmatched_;
  Tuple current_right_tuple_;                         // 当前右表元组
  std::vector<Tuple> *current_left_tuples_{nullptr};  // 当前右表元组对应的所有左表元组
  size_t current_left_tuple_idx_ = 0;

  /** Utility method to generate the join key */
  auto MakeJoinKey(const Tuple &tuple, const std::vector<AbstractExpressionRef> &key_exprs, const Schema *schema)
      -> std::vector<Value>;
};

}  // namespace bustub
