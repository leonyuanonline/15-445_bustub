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

#include "concurrency/transaction_manager.h"
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
  auto txn = exec_ctx_->GetTransaction();
  auto txn_id = txn->GetTransactionId();
  auto txn_tmp_ts = TXN_START_ID | (txn_id - TXN_START_ID);

  // 获取表的所有索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // 首先扫描所有要删除的元组到本地缓冲区
  std::vector<std::pair<Tuple, RID>> tuples_to_delete;
  Tuple child_tuple;
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_to_delete.emplace_back(child_tuple, child_rid);
  }

  for (const auto &[tuple_to_delete, tuple_rid] : tuples_to_delete) {
    // 获取元组的元数据和当前内容
    auto [meta, current_tuple] = table_info_->table_->GetTuple(tuple_rid);

    // 检查写写冲突
    auto write_sets = txn->GetWriteSets();
    if (meta.ts_ > txn->GetReadTs() &&
        write_sets[table_info_->oid_].find(tuple_rid) == write_sets[table_info_->oid_].end()) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected");
    }
    LOG_DEBUG("current_rid = %s, ts = %ld, is_deleted = %d", tuple_rid.ToString().c_str(), meta.ts_, meta.is_deleted_);
    // 处理自我修改的情况
    if (write_sets[table_info_->oid_].find(tuple_rid) != write_sets[table_info_->oid_].end()) {
      // 直接更新元组元数据
      meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(meta, tuple_rid);

      // 如果存在撤销日志，更新它
      // auto txn_mgr = exec_ctx_->GetTransactionManager();
      // auto version_link = txn_mgr->GetVersionLink(tuple_rid);
      // LOG_DEBUG("version_link is valid %d", version_link.has_value());
      // if (version_link.has_value()) {
      //   auto undo_log = txn_mgr->GetUndoLog(version_link->prev_);
      //   std::vector<Column> new_columns;
      //   std::vector<Column> old_columns;
      //   std::vector<Value> values;
      //   for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
      //     undo_log.modified_fields_[i] = true;
      //     if (undo_log.modified_fields_[i]) {
      //       old_columns.push_back(table_info_->schema_.GetColumn(i));
      //       new_columns.push_back(table_info_->schema_.GetColumn(i));

      //       auto schema = std::make_unique<Schema>(old_columns);
      //       values.push_back(undo_log.tuple_.GetValue(schema.get(), old_columns.size() - 1));
      //     } else {
      //       new_columns.push_back(table_info_->schema_.GetColumn(i));
      //       values.push_back(tuple_to_delete.GetValue(&table_info_->schema_, i));
      //     }
      //   }
      //   auto schema = std::make_unique<Schema>(new_columns);
      //   auto new_tuple = Tuple(values, schema.get());
      //   undo_log.is_deleted_ = true;
      //   txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, undo_log);
      // }
    } else {
      // 生成撤销日志
      UndoLog undo_log;
      undo_log.is_deleted_ = false;
      undo_log.ts_ = meta.ts_;

      // 记录完整的元组内容用于恢复
      undo_log.tuple_ = tuple_to_delete;
      for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
        undo_log.modified_fields_.push_back(true);  // 删除操作影响所有字段
      }

      // 添加撤销日志并更新链接
      auto txn_mgr = exec_ctx_->GetTransactionManager();
      auto old_undo_link = txn_mgr->GetUndoLink(tuple_rid);
      if (old_undo_link.has_value()) {
        undo_log.prev_version_ = old_undo_link.value();
      }
      auto prev_log = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateUndoLink(tuple_rid, prev_log, nullptr);

      // 更新元组元数据
      meta.ts_ = txn_tmp_ts;
      meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(meta, tuple_rid);
    }

    // 将RID添加到写集
    txn->AppendWriteSet(table_info_->oid_, tuple_rid);

    // 删除相关的索引条目
    for (const auto &index_info : indexes) {
      auto mutable_tuple_to_delete = tuple_to_delete;
      auto key_tuple = mutable_tuple_to_delete.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                            index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, tuple_rid, txn);
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
