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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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
  LOG_DEBUG("SeqScanExecutor::Next called");
  // Get current transaction's info
  auto txn = exec_ctx_->GetTransaction();
  auto read_ts = txn->GetReadTs();
  auto txn_id = txn->GetTransactionId();
  auto txn_tmp_ts = TXN_START_ID + (txn_id - TXN_START_ID);
  LOG_DEBUG("txn_tmp_ts = %ld, read_ts %ld, txn id %ld", txn_tmp_ts, read_ts, txn_id ^ TXN_START_ID);

  while (!iter_->IsEnd()) {
    // Get current tuple and metadata
    auto [meta, current_tuple] = iter_->GetTuple();
    auto current_rid = iter_->GetRID();

    // Track if we should return this tuple
    bool should_return = false;
    Tuple output_tuple = current_tuple;

    LOG_DEBUG("current_rid = %s, ts = %ld, is_deleted = %d", current_rid.ToString().c_str(), meta.ts_,
              meta.is_deleted_);

    if (meta.is_deleted_) {
      if (meta.ts_ == txn_tmp_ts) {
        should_return = false;
      } else {
        if (meta.ts_ <= read_ts) {
          should_return = false;
        } else {
          // Get version chain
          auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
          auto txn_mgr = exec_ctx_->GetTransactionManager();

          // Collect all undo logs up to read timestamp
          std::vector<UndoLog> undo_logs;
          auto undo_link = txn_mgr->GetUndoLink(current_rid);
          auto last_undo_log_ts = INVALID_TS;

          LOG_DEBUG("undo_link is valid %d", undo_link->IsValid());
          while (undo_link.has_value() && undo_link->IsValid()) {
            auto undo_log = txn_mgr->GetUndoLog(*undo_link);
            LOG_DEBUG("undo_log.ts_ = %ld", undo_log.ts_);
            // if (undo_log.ts_ >= read_ts) {
            //   undo_logs.push_back(undo_log);
            // }
            if (last_undo_log_ts != INVALID_TS && undo_log.ts_ < read_ts && last_undo_log_ts <= read_ts) {
              break;
            }
            undo_logs.push_back(undo_log);
            last_undo_log_ts = undo_log.ts_;
            if (undo_log.ts_ < txn_mgr->GetWatermark()) {
              break;
            }
            undo_link = undo_log.prev_version_;
          }
          if (last_undo_log_ts > read_ts) {
            undo_logs.clear();
          }

          LOG_DEBUG("undo_logs is empty %d", undo_logs.empty());
          if (!undo_logs.empty()) {
            // Reconstruct the tuple at read timestamp
            auto maybe_tuple = ReconstructTuple(&table_info->schema_, current_tuple, meta, undo_logs);
            if (maybe_tuple.has_value()) {
              output_tuple = std::move(*maybe_tuple);
              should_return = true;
            }
          }
        }
      }
    } else {
      if (meta.ts_ == txn_tmp_ts) {
        should_return = true;
      } else {
        if (meta.ts_ <= read_ts) {
          should_return = true;
        } else {
          // Get version chain
          auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
          auto txn_mgr = exec_ctx_->GetTransactionManager();

          // Collect all undo logs up to read timestamp
          std::vector<UndoLog> undo_logs;
          auto undo_link = txn_mgr->GetUndoLink(current_rid);
          auto last_undo_log_ts = INVALID_TS;

          LOG_DEBUG("undo_link is valid %d", undo_link->IsValid());
          while (undo_link.has_value() && undo_link->IsValid()) {
            auto undo_log = txn_mgr->GetUndoLog(*undo_link);
            LOG_DEBUG("undo_log.ts_ = %ld", undo_log.ts_);
            // if (undo_log.ts_ >= read_ts) {
            //   undo_logs.push_back(undo_log);
            // }
            if (last_undo_log_ts != INVALID_TS && undo_log.ts_ < read_ts && last_undo_log_ts <= read_ts) {
              break;
            }
            undo_logs.push_back(undo_log);
            last_undo_log_ts = undo_log.ts_;
            if (undo_log.ts_ < txn_mgr->GetWatermark()) {
              break;
            }
            undo_link = undo_log.prev_version_;
          }
          if (last_undo_log_ts > read_ts) {
            undo_logs.clear();
          }

          LOG_DEBUG("undo_logs is empty %d", undo_logs.empty());
          if (!undo_logs.empty()) {
            // Reconstruct the tuple at read timestamp
            auto maybe_tuple = ReconstructTuple(&table_info->schema_, current_tuple, meta, undo_logs);
            if (maybe_tuple.has_value()) {
              output_tuple = std::move(*maybe_tuple);
              should_return = true;
            }
          }
        }
      }
    }

    ++(*iter_);  // Advance iterator

    if (should_return) {
      // Apply predicate if exists
      if (plan_->filter_predicate_ == nullptr ||
          plan_->filter_predicate_->Evaluate(&output_tuple, GetOutputSchema()).GetAs<bool>()) {
        *tuple = output_tuple;
        *rid = current_rid;
        return true;
      }
    }
  }

  return false;
}

}  // namespace bustub
