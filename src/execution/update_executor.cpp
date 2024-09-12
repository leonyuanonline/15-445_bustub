//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  // 初始化子执行器
  child_executor_->Init();

  // 获取要更新的表的信息
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  if (table_info_ == nullptr) {
    throw std::runtime_error("Table not found.");
  }
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    // 如果已经执行过更新操作，返回 false
    return false;
  }

  int32_t rows_updated = 0;

  // 获取表的所有索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // 从子执行器获取要更新的元组
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 构建新的元组
    std::vector<Value> updated_values;
    updated_values.reserve(plan_->target_expressions_.size());

    for (const auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&child_tuple, table_info_->schema_));
    }

    Tuple updated_tuple(updated_values, &table_info_->schema_);

    // 获取元组的元数据
    auto [meta, current_tuple] = table_info_->table_->GetTuple(child_rid);

    // 标记旧元组为删除
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, child_rid);

    // 插入新的元组
    auto insert_rid = table_info_->table_->InsertTuple({0, false}, updated_tuple, exec_ctx_->GetLockManager(),
                                                       exec_ctx_->GetTransaction());
    if (!insert_rid.has_value()) {
      throw std::runtime_error("Failed to insert updated tuple.");
    }

    // 更新所有相关的索引
    for (const auto &index_info : indexes) {
      // 删除旧索引
      auto old_key_tuple =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key_tuple, child_rid, exec_ctx_->GetTransaction());

      // 插入新索引
      auto new_key_tuple =
          updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key_tuple, insert_rid.value(), exec_ctx_->GetTransaction());
    }

    rows_updated++;
  }

  // 返回更新的行数作为结果
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(rows_updated));
  *tuple = Tuple(values, &GetOutputSchema());

  // 标记为已经执行更新
  has_updated_ = true;

  return true;
}

}  // namespace bustub
