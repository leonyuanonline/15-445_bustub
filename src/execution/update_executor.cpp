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

#include "concurrency/transaction_manager.h"
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
  auto txn = exec_ctx_->GetTransaction();
  auto txn_id = txn->GetTransactionId();
  auto txn_tmp_ts = TXN_START_ID | (txn_id - TXN_START_ID);
  LOG_DEBUG("called, txn_id = %ld, txn_tmp_ts = %ld", txn_id, txn_tmp_ts);

  // 获取表的所有索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // 首先扫描所有要更新的元组到本地缓冲区
  std::vector<std::pair<Tuple, RID>> tuples_to_update;
  Tuple child_tuple;
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_to_update.emplace_back(child_tuple, child_rid);
  }
  LOG_DEBUG("tuples_to_update size = %lu", tuples_to_update.size());

  for (const auto &[old_tuple, tuple_rid] : tuples_to_update) {
    // 获取元组的元数据和当前内容
    auto [meta, current_tuple] = table_info_->table_->GetTuple(tuple_rid);

    // 检查写写冲突
    auto write_sets = txn->GetWriteSets();
    if (meta.ts_ > txn->GetReadTs() &&
        write_sets[table_info_->oid_].find(tuple_rid) == write_sets[table_info_->oid_].end()) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected");
    }

    // 构建新的元组内容
    std::vector<Value> updated_values;
    updated_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      updated_values.push_back(expr->Evaluate(&old_tuple, table_info_->schema_));
    }
    Tuple new_tuple(updated_values, &table_info_->schema_);
    LOG_DEBUG("ole_tuple = %s", old_tuple.ToString(&table_info_->schema_).c_str());
    LOG_DEBUG("new_tuple = %s", new_tuple.ToString(&table_info_->schema_).c_str());

    // 处理自我修改的情况
    if (write_sets[table_info_->oid_].find(tuple_rid) != write_sets[table_info_->oid_].end()) {
      // 直接更新表堆中的元组
      table_info_->table_->UpdateTupleInPlace(meta, new_tuple, tuple_rid, nullptr);

      // 如果存在撤销日志，更新它
      auto txn_mgr = exec_ctx_->GetTransactionManager();
      auto version_link = txn_mgr->GetVersionLink(tuple_rid);
      if (version_link.has_value()) {
        auto undo_log = txn_mgr->GetUndoLog(version_link->prev_);
        std::vector<Column> new_columns;
        std::vector<Column> old_columns;
        std::vector<Value> values;
        for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
          if (undo_log.modified_fields_[i] ||
              !old_tuple.GetValue(&table_info_->schema_, i)
                   .CompareExactlyEquals(new_tuple.GetValue(&table_info_->schema_, i))) {
            new_columns.push_back(table_info_->schema_.GetColumn(i));
            if (undo_log.modified_fields_[i]) {
              old_columns.push_back(table_info_->schema_.GetColumn(i));
              auto schema = std::make_unique<Schema>(old_columns);
              values.push_back(undo_log.tuple_.GetValue(schema.get(), old_columns.size() - 1));
            } else {
              undo_log.modified_fields_[i] = true;
              values.push_back(old_tuple.GetValue(&table_info_->schema_, i));
            }
          }
        }
        auto schema = std::make_unique<Schema>(new_columns);
        undo_log.tuple_ = Tuple(values, schema.get());
        txn->ModifyUndoLog(version_link->prev_.prev_log_idx_, undo_log);
      }
    } else {
      // 生成撤销日志
      LOG_DEBUG("generate undo log");
      UndoLog undo_log;
      undo_log.is_deleted_ = false;
      undo_log.ts_ = meta.ts_;
      LOG_DEBUG("set undo log ts to %ld", undo_log.ts_);

      std::vector<Value> undo_log_values;
      std::vector<Column> columns;
      // 组装undo log, 只记录修改的列
      for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
        LOG_DEBUG("col %d", i);
        LOG_DEBUG("old value %s", old_tuple.GetValue(&table_info_->schema_, i).ToString().c_str());
        LOG_DEBUG("new value %s", new_tuple.GetValue(&table_info_->schema_, i).ToString().c_str());
        if (!old_tuple.GetValue(&table_info_->schema_, i)
                 .CompareExactlyEquals(new_tuple.GetValue(&table_info_->schema_, i))) {
          LOG_DEBUG("col %d is modified", i);
          undo_log.modified_fields_.push_back(true);
          undo_log_values.push_back(old_tuple.GetValue(&table_info_->schema_, i));
          columns.push_back(table_info_->schema_.GetColumn(i));
        } else {
          LOG_DEBUG("col  %d is not modified", i);
          undo_log.modified_fields_.push_back(false);
        }
      }
      auto undo_schema = std::make_unique<Schema>(columns);
      undo_log.tuple_ = Tuple(undo_log_values, undo_schema.get());

      auto txn_mgr = exec_ctx_->GetTransactionManager();
      auto old_undo_link = txn_mgr->GetUndoLink(tuple_rid);
      if (old_undo_link.has_value()) {
        undo_log.prev_version_ = old_undo_link.value();
      }
      auto prev_log = txn->AppendUndoLog(undo_log);
      txn_mgr->UpdateUndoLink(tuple_rid, prev_log, nullptr);

      // 更新表堆中的元组
      meta.ts_ = txn_tmp_ts;
      table_info_->table_->UpdateTupleInPlace(meta, new_tuple, tuple_rid, nullptr);
    }
    // 将RID添加到写集
    txn->AppendWriteSet(table_info_->oid_, tuple_rid);

    // 更新索引
    for (const auto &index_info : indexes) {
      Tuple mutable_old_tuple = old_tuple;
      auto old_key = mutable_old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                    index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, tuple_rid, txn);

      auto new_key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key, tuple_rid, txn);
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
