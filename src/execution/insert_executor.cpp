//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // 初始化子执行器
  child_executor_->Init();

  // 获取要插入的表的信息
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  if (table_info_ == nullptr) {
    throw std::runtime_error("Table not found.");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    // 如果已经执行过插入操作，返回 false
    return false;
  }

  int32_t rows_inserted = 0;

  // 获取表的所有索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // 获取当前事务
  auto txn = exec_ctx_->GetTransaction();
  auto txn_id = txn->GetTransactionId();
  auto txn_tmp_ts = TXN_START_ID | (txn_id - TXN_START_ID);
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  LOG_DEBUG("txn_id = %ld, txn_tmp_ts = %ld", txn_id, txn_tmp_ts);

  // 从子执行器获取要插入的元组
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::cout << "Retrieved tuple: " << child_tuple.ToString(&table_info_->schema_) << std::endl;
    TupleMeta meta{};
    meta.is_deleted_ = false;
    meta.ts_ = txn_tmp_ts;  // 设置为事务的临时时间戳

    // 下一个版本链接设置为 nullopt

    // 将元组插入表中
    auto insert_rid = table_info_->table_->InsertTuple(meta, child_tuple, exec_ctx_->GetLockManager(), txn);

    if (!insert_rid.has_value()) {
      throw std::runtime_error("Failed to insert tuple.");
    }

    txn->AppendWriteSet(table_info_->oid_, insert_rid.value());
    // 更新所有相关的索引
    for (const auto &index_info : indexes) {
      auto key_tuple =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, insert_rid.value(), exec_ctx_->GetTransaction());
    }

    txn_mgr->UpdateUndoLink(insert_rid.value(), std::nullopt, nullptr);

    auto version_link = txn_mgr->GetVersionLink(insert_rid.value());
    LOG_DEBUG("version_link is valid %d", version_link.has_value());
    auto undo_link = version_link->prev_;
    fmt::println("RID={}/{}", insert_rid.value().GetPageId(), insert_rid.value().GetSlotNum());
    LOG_DEBUG("undo_link is valid %d", undo_link.IsValid());
    rows_inserted++;
  }
  LOG_DEBUG("rows inserted %d", rows_inserted);
  // 返回插入的行数作为结果
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(rows_inserted));
  *tuple = Tuple(values, &GetOutputSchema());

  // 标记为已经执行插入
  has_inserted_ = true;

  return true;
}

}  // namespace bustub
