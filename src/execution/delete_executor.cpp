//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // 初始化子执行器
  child_executor_->Init();

  // 获取要删除的表的信息
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  if (table_info_ == nullptr) {
    throw std::runtime_error("Table not found.");
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    // 如果已经执行过删除操作，返回 false
    return false;
  }

  int32_t rows_deleted = 0;

  // 获取表的所有索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // 从子执行器获取要删除的元组
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 获取元组的元数据
    auto [meta, current_tuple] = table_info_->table_->GetTuple(child_rid);

    // 标记元组为删除
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, child_rid);

    // 删除相关的索引条目
    for (const auto &index_info : indexes) {
      auto key_tuple =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, child_rid, exec_ctx_->GetTransaction());
    }

    rows_deleted++;
  }

  // 返回删除的行数作为结果
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(rows_deleted));
  *tuple = Tuple(values, &GetOutputSchema());

  // 标记为已经执行删除
  has_deleted_ = true;

  return true;
}
}  // namespace bustub
