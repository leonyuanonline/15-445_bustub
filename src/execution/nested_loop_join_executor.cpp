//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  LOG_DEBUG(" called");
  left_executor_->Init();
  RID temp_rid;  // 使用临时的 RID 变量
  left_tuple_ready_ = left_executor_->Next(&left_tuple_, &temp_rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_tuple_ready_) {
    Tuple right_tuple;
    RID temp_rid;
    LOG_DEBUG("next call left join emitted %d", left_join_emitted_);
    if (!right_executor_initialized_) {
      right_executor_->Init();
      right_executor_initialized_ = true;
    }
    // 遍历右表
    while (right_executor_->Next(&right_tuple, &temp_rid)) {
      LOG_DEBUG("right executor next exist");
      // 评估连接条件
      auto predicate = plan_->Predicate();
      if (predicate == nullptr || (!predicate
                                        ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                       right_executor_->GetOutputSchema())
                                        .IsNull() &&
                                   predicate
                                       ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                      right_executor_->GetOutputSchema())
                                       .GetAs<bool>())) {
        // 如果谓词为真，则生成连接的元组
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        left_join_emitted_ = true;
        LOG_DEBUG("set left join emitted 1");
        return true;
      }
      LOG_DEBUG("join failed");
    }

    // 如果是左连接且没有找到匹配的右表元组，且还未为当前左表元组生成输出
    if (!left_join_emitted_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      left_join_emitted_ = true;
      LOG_DEBUG("set left join emitted 1");
      return true;
    }

    // 获取下一个左表元组，并重置右表
    left_tuple_ready_ = left_executor_->Next(&left_tuple_, &temp_rid);
    LOG_DEBUG("set left join emitted 0");
    right_executor_initialized_ = false;
    left_join_emitted_ = false;
  }

  return false;
}

}  // namespace bustub
