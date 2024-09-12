//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  result_rids_.clear();
  // 从系统目录获取索引信息
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  if (index_info == nullptr) {
    throw std::runtime_error("Index not found.");
  }

  // 获取表的信息
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  if (table_info_ == nullptr) {
    throw std::runtime_error("Table not found.");
  }

  // 将索引对象动态转换为 HashTableIndexForTwoIntegerColumn 类型
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  if (htable_ == nullptr) {
    throw std::runtime_error("Failed to cast index to HashTableIndexForTwoIntegerColumn.");
  }

  // 如果有点查找的键，则执行点查找
  if (plan_->pred_key_ != nullptr) {
    // 创建单列的Schema
    std::vector<Column> columns;
    columns.emplace_back("col", plan_->pred_key_->GetReturnType());
    Schema key_schema(columns);

    // 使用 Schema 和 Value 构建 Tuple
    std::vector<Value> key_values = {plan_->pred_key_->Evaluate(nullptr, key_schema)};
    Tuple key_tuple(key_values, &key_schema);

    // 在索引中查找匹配的 RID
    htable_->ScanKey(key_tuple, &result_rids_, exec_ctx_->GetTransaction());
  } else {
    // 如果没有点查找键，抛出异常或者你可以处理其他扫描类型
    throw std::runtime_error("IndexScanExecutor only supports point lookup with pred_key_");
  }

  // 初始化迭代器
  iter_ = result_rids_.begin();
  end_ = result_rids_.end();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果迭代结束，返回 false
  if (iter_ == end_) {
    return false;
  }

  // 获取当前 RID，并前进迭代器
  *rid = *iter_;
  ++iter_;

  // 从表堆中检索元组
  auto [meta, found_tuple] = table_info_->table_->GetTuple(*rid);

  // 如果元组未被删除，返回元组
  if (!meta.is_deleted_) {
    *tuple = found_tuple;
    return true;
  }

  // 否则继续获取下一个元组
  return Next(tuple, rid);
}

}  // namespace bustub
