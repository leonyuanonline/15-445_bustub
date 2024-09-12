//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(std::nullopt) {}

void SeqScanExecutor::Init() {
  LOG_DEBUG("SeqScanExecutor::Init called");
  // 获取表的元数据信息
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  // 初始化表堆的迭代器，使用起始 RID 和结束 RID 来构造 TableIterator
  iter_.emplace(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!iter_.has_value() || iter_->IsEnd()) {
    return false;
  }

  while (!iter_->IsEnd()) {
    // 从迭代器中获取当前元组及其元数据
    auto [meta, current_tuple] = iter_->GetTuple();
    *tuple = current_tuple;
    *rid = iter_->GetRID();  // 仅在 rid 不为 nullptr 时更新 RID

    // 递增迭代器
    ++(*iter_);

    // 检查元组是否已被删除，忽略已删除的元组
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ == nullptr ||
          plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema()).GetAs<bool>()) {
        return true;
      }
    }
  }

  // 如果没有更多元组，返回 false
  return false;
}

}  // namespace bustub
