//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  // 初始化子执行器
  child_executor_->Init();
  produced_count_ = 0;  // 重置已产生的元组计数器
  LOG_DEBUG("LimitExecutor initialized with limit = %ld", plan_->GetLimit());
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 检查是否已经达到了限制数
  if (produced_count_ >= plan_->GetLimit()) {
    LOG_DEBUG("Limit reached: %ld of %ld tuples produced.", produced_count_, plan_->GetLimit());
    return false;  // 如果已经产生了足够的元组，则返回 false，表示没有更多元组了
  }

  // 从子执行器获取下一个元组
  if (child_executor_->Next(tuple, rid)) {
    produced_count_++;  // 增加已产生的元组计数
    LOG_DEBUG("Produced tuple: %s", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }

  // 如果子执行器没有更多的元组，则返回 false
  LOG_DEBUG("Child executor has no more tuples to produce.");
  return false;
}

}  // namespace bustub
