//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstring>

namespace bustub {

// define a macro to print the content of key (std::vector<Value>)
#define PRINT_KEY(key)                    \
  do {                                    \
    for (auto val : (key)) {              \
      std::cout << val.ToString() << " "; \
    }                                     \
    std::cout << std::endl;               \
  } while (0)

auto AreTuplesEqual(const Tuple &tuple1, const Tuple &tuple2, const Schema *schema) -> bool {
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    Value val1 = tuple1.GetValue(schema, i);
    Value val2 = tuple2.GetValue(schema, i);

    // 如果两个值都是 NULL，则认为它们相等
    if (val1.IsNull() && val2.IsNull()) {
      continue;
    }

    // 如果一个值为 NULL，另一个值不为 NULL，则不相等
    if (val1.IsNull() || val2.IsNull()) {
      return false;
    }

    // 对于非 NULL 值进行实际的比较
    if (val1.CompareEquals(val2) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // 初始化子执行器
  left_child_->Init();
  right_child_->Init();

  // 构建哈希表
  Tuple left_tuple;
  RID left_rid;

  const Schema left_schema = left_child_->GetOutputSchema();

  while (left_child_->Next(&left_tuple, &left_rid)) {
    // 获取左表的连接键
    auto left_key = MakeJoinKey(left_tuple, plan_->LeftJoinKeyExpressions(), &left_schema);
    // 插入哈希表
    hash_table_.Insert(left_key, left_tuple);
    left_tuples_unmatched_.push_back(UnmatchedTuple{left_tuple, false});
    LOG_DEBUG("Inserted into hash table: ");
    PRINT_KEY(left_key);
    LOG_DEBUG("tuple = %s", left_tuple.ToString(&left_schema).c_str());
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("called");
  const Schema left_schema = left_child_->GetOutputSchema();
  const Schema right_schema = right_child_->GetOutputSchema();

  // 1. 首先处理当前右表元组和左表元组的匹配
  if (current_left_tuples_ != nullptr && current_left_tuple_idx_ < current_left_tuples_->size()) {
    // 获取当前匹配的左表元组
    const auto &left_tuple = (*current_left_tuples_)[current_left_tuple_idx_++];
    std::vector<Value> values;

    // 合并左表和右表的元组
    for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
      values.push_back(left_tuple.GetValue(&left_schema, i));
    }
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
      values.push_back(current_right_tuple_.GetValue(&right_schema, i));
    }

    *tuple = Tuple(values, &GetOutputSchema());
    LOG_DEBUG("return tuple = %s", tuple->ToString(&GetOutputSchema()).c_str());

    // 标记当前左表元组为已匹配
    for (auto &unmatched_tuple : left_tuples_unmatched_) {
      if (unmatched_tuple.matched_) {
        continue;
      }
      if (AreTuplesEqual(unmatched_tuple.tuple_, left_tuple, &left_schema)) {
        LOG_DEBUG("set tuple matched rid %d tuple %s", unmatched_tuple.tuple_.GetRid().GetSlotNum(),
                  unmatched_tuple.tuple_.ToString(&left_schema).c_str());
        unmatched_tuple.matched_ = true;
        break;
      }
    }

    return true;  // 返回当前匹配的组合
  }

  Tuple right_tuple;
  RID right_rid;

  // 2. 查找下一个右表元组并匹配对应的左表元组
  while (right_child_->Next(&right_tuple, &right_rid)) {
    current_right_tuple_ = right_tuple;
    auto right_key = MakeJoinKey(current_right_tuple_, plan_->RightJoinKeyExpressions(), &right_schema);
    LOG_DEBUG("right key = ");
    PRINT_KEY(right_key);
    current_left_tuples_ = hash_table_.Find(right_key);
    current_left_tuple_idx_ = 0;

    if (current_left_tuples_ != nullptr && !current_left_tuples_->empty()) {
      // 找到匹配的左表元组集合，直接处理第一个匹配
      const auto &left_tuple = (*current_left_tuples_)[current_left_tuple_idx_++];
      std::vector<Value> values;

      // 合并左表和右表的元组
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(current_right_tuple_.GetValue(&right_schema, i));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      LOG_DEBUG("left tuple %s rid %d", left_tuple.ToString(&left_schema).c_str(), left_tuple.GetRid().GetSlotNum());
      // 标记当前左表元组为已匹配
      for (auto &unmatched_tuple : left_tuples_unmatched_) {
        if (unmatched_tuple.matched_) {
          continue;
        }
        if (AreTuplesEqual(unmatched_tuple.tuple_, left_tuple, &left_schema)) {
          LOG_DEBUG("set tuple matched rid %d tuple %s", unmatched_tuple.tuple_.GetRid().GetSlotNum(),
                    unmatched_tuple.tuple_.ToString(&left_schema).c_str());
          unmatched_tuple.matched_ = true;
          break;
        }
      }
      LOG_DEBUG("return tuple = %s", tuple->ToString(&GetOutputSchema()).c_str());
      return true;
    }
  }

  // 3. 处理所有右表元组后，检查未匹配的左表元组
  if (plan_->GetJoinType() == JoinType::LEFT) {
    for (auto &unmatched_tuple : left_tuples_unmatched_) {
      if (!unmatched_tuple.matched_) {
        std::vector<Value> values;
        LOG_DEBUG("found unmatched tuple = %s", unmatched_tuple.tuple_.ToString(&left_schema).c_str());
        // 填充左表的值
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
          values.push_back(unmatched_tuple.tuple_.GetValue(&left_schema, i));
        }

        // 填充右表的值为 NULL
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
        }

        *tuple = Tuple(values, &GetOutputSchema());
        LOG_DEBUG("set tuple matched rid %d tuple %s", unmatched_tuple.tuple_.GetRid().GetSlotNum(),
                  unmatched_tuple.tuple_.ToString(&left_schema).c_str());
        unmatched_tuple.matched_ = true;  // 标记为已处理
        LOG_DEBUG("return tuple = %s", tuple->ToString(&GetOutputSchema()).c_str());
        return true;
      }
    }
  }

  return false;  // 没有更多的元组可以处理了
}

auto HashJoinExecutor::MakeJoinKey(const Tuple &tuple, const std::vector<AbstractExpressionRef> &key_exprs,
                                   const Schema *schema) -> std::vector<Value> {
  std::vector<Value> key(key_exprs.size());
  for (size_t i = 0; i < key_exprs.size(); ++i) {
    key[i] = key_exprs[i]->Evaluate(&tuple, *schema);
  }
  return key;
}

}  // namespace bustub
