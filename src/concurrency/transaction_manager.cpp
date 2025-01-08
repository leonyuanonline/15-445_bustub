//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <algorithm>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  txn->commit_ts_ = last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  const auto &write_sets = txn->GetWriteSets();
  for (const auto &[table_oid, rid_set] : write_sets) {
    // 获取表信息
    auto table_info = catalog_->GetTable(table_oid);
    if (table_info == nullptr) {
      throw Exception("Table not found during commit");
    }

    // 遍历该表中所有修改过的RID
    for (const auto &rid : rid_set) {
      // 获取当前元组的元数据
      auto tuple_meta = table_info->table_->GetTupleMeta(rid);

      // 更新元组的时间戳为提交时间戳
      tuple_meta.ts_ = txn->commit_ts_.load();

      // 将更新后的元数据写回表中
      table_info->table_->UpdateTupleMeta(tuple_meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::COMMITTED;

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  // Update the last committed timestamp if necessary
  while (true) {
    auto current = last_commit_ts_.load();
    if (last_commit_ts_.compare_exchange_weak(current, std::max(current, txn->commit_ts_.load()))) {
      break;
    }
  }

  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // std::lock_guard<std::shared_mutex> guard(txn_map_mutex_);

  // 获取水印(最低读取时间戳)
  auto watermark = running_txns_.GetWatermark();
  LOG_DEBUG("watermark=%ld", watermark);

  // 收集每个事务的所有撤销日志时间戳
  std::unordered_map<txn_id_t, std::unordered_set<timestamp_t>> txn_visible_ts;

  // 获取version_info的读锁
  std::shared_lock<std::shared_mutex> version_info_lock(version_info_mutex_);
  LOG_DEBUG("version_info_.size()=%lu", version_info_.size());

  // 遍历所有页面的版本信息
  for (const auto &[page_id, page_version_info] : version_info_) {
    // std::shared_lock<std::shared_mutex> page_lock(page_version_info->mutex_);

    // 遍历页面中所有slot的版本链
    for (const auto &[slot_id, version] : page_version_info->prev_version_) {
      LOG_DEBUG("slot_id=%lu", slot_id);
      // 从第一个版本开始遍历整个版本链
      auto undo_link = version.prev_;
      while (undo_link.IsValid()) {
        if (txn_map_.find(undo_link.prev_txn_) == txn_map_.end()) {
          break;
        }
        auto txn = txn_map_.find(undo_link.prev_txn_)->second;
        // 获取当前版本的撤销日志
        auto undo_log = GetUndoLog(undo_link);
        LOG_DEBUG("undo_log.ts_=%ld", undo_log.ts_);

        // 如果撤销日志的时间戳大于等于水印,说明这个撤销日志仍可能被访问
        if (undo_log.ts_ >= watermark || txn->GetReadTs() >= watermark) {
          LOG_DEBUG("add txn_id=%lu, ts=%ld to visiable", undo_link.prev_txn_ ^ TXN_START_ID, undo_log.ts_);
          txn_visible_ts[undo_link.prev_txn_].insert(undo_log.ts_);
        }

        // 获取下一个
        undo_link = undo_log.prev_version_;
      }
    }
  }

  // 遍历所有事务,识别可以回收的事务
  std::vector<txn_id_t> txns_to_remove;
  for (const auto &[txn_id, txn] : txn_map_) {
    // 跳过未完成的事务
    if (txn->GetTransactionState() != TransactionState::COMMITTED &&
        txn->GetTransactionState() != TransactionState::ABORTED) {
      continue;
    }
    LOG_DEBUG("txn_id=%lu", txn_id ^ TXN_START_ID);
    // 如果事务没有任何大于等于水印的撤销日志时间戳,可以回收它
    if (txn_visible_ts.find(txn_id) == txn_visible_ts.end()) {
      LOG_DEBUG("txn_id=%lu can be removed", txn_id ^ TXN_START_ID);
      txns_to_remove.push_back(txn_id);
      continue;
    }

    // 或者虽然有撤销日志,但所有时间戳都小于水印
    // const auto &ts_set = txn_visible_ts[txn_id];
    // if (ts_set.empty() || *std::prev(ts_set.end()) < watermark) {
    //   txns_to_remove.push_back(txn_id);
    // }
  }

  // 移除可以回收的事务
  for (auto txn_id : txns_to_remove) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
