#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // UNIMPLEMENTED("not implemented");
  // If the base tuple is deleted and no undo logs, nothing to reconstruct
  if (base_meta.is_deleted_ && undo_logs.empty()) {
    return std::nullopt;
  }

  // Start with the base tuple values
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }

  // Apply undo logs in reverse chronological order
  bool is_deleted = base_meta.is_deleted_;
  for (const auto &undo_log : undo_logs) {
    // Update deletion status
    if (undo_log.is_deleted_) {
      is_deleted = true;
      continue;
    }
    if (is_deleted) {
      // This undo log undeletes the tuple
      is_deleted = false;
    }

    // If still deleted after this log, no need to apply changes
    if (is_deleted) {
      continue;
    }

    // Get the partial schema for this undo log
    std::vector<Column> partial_columns;
    size_t partial_idx = 0;

    // Create mapping between partial tuple columns and full schema
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (undo_log.modified_fields_[i]) {
        partial_columns.push_back(schema->GetColumn(i));
      }
    }
    Schema partial_schema(partial_columns);

    // Apply changes from the undo log
    partial_idx = 0;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (undo_log.modified_fields_[i]) {
        values[i] = undo_log.tuple_.GetValue(&partial_schema, partial_idx);
        partial_idx++;
      }
    }
  }

  // Return nullopt if the final state is deleted
  if (is_deleted) {
    return std::nullopt;
  }

  return Tuple{values, schema};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
  // Traverse the table heap and print the version chain for each record
  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    // Get the current RID and tuple
    RID rid = iter.GetRID();
    auto [meta, tuple] = iter.GetTuple();

    // Print the current tuple information
    fmt::print(stderr, "RID={}/{} ts={}{} {}tuple=(", rid.GetPageId(), rid.GetSlotNum(),
               (meta.ts_ & (1ULL << 62)) != 0ULL ? "txn" : "",
               (meta.ts_ & (1ULL << 62)) != 0ULL ? meta.ts_ - TXN_START_ID : meta.ts_,
               meta.is_deleted_ ? "<del marker> " : "");
    for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
      if (i > 0) {
        fmt::print(stderr, ", ");
      }
      fmt::print(stderr, "{}", tuple.GetValue(&table_info->schema_, i).ToString());
    }
    fmt::print(stderr, ")\n");

    // Traverse the version chain
    auto version_link = txn_mgr->GetVersionLink(rid);
    LOG_DEBUG("version_link is valid %d", version_link.has_value());
    if (version_link.has_value()) {
      auto undo_link = version_link->prev_;
      LOG_DEBUG("undo_link is valid %d", undo_link.IsValid());
      while (undo_link.IsValid()) {
        std::vector<Column> columns;
        LOG_DEBUG("prev_txn_ = %ld, prev_log_idx_ = %d", undo_link.prev_txn_, undo_link.prev_log_idx_);
        // Get the transaction and compare its read timestamp and watermark
        auto item = txn_mgr->txn_map_.find(undo_link.prev_txn_);
        if (item == txn_mgr->txn_map_.end()) {
          break;
        }
        auto txn = item->second;
        if (txn->GetReadTs() < txn_mgr->GetWatermark()) {
          break;
        }

        auto undo_log = txn_mgr->GetUndoLog(undo_link);
        fmt::print(stderr, "  txn{}@{} ", undo_link.prev_txn_ ^ TXN_START_ID, undo_link.prev_log_idx_);
        if (undo_log.is_deleted_) {
          fmt::print(stderr, "<del>");
        } else {
          fmt::print(stderr, "(");
          for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
            if (i > 0) {
              fmt::print(stderr, ", ");
            }
            if (undo_log.modified_fields_[i]) {
              columns.push_back(table_info->schema_.GetColumn(i));
              auto schema = std::make_unique<Schema>(columns);
              fmt::print(stderr, "{}", undo_log.tuple_.GetValue(schema.get(), columns.size() - 1).ToString());
            } else {
              fmt::print(stderr, "_");
            }
          }
          fmt::print(stderr, ")");
        }
        fmt::print(stderr, " ts={}\n", undo_log.ts_);
        undo_link = undo_log.prev_version_;
      }
    }

    // Move to the next RID
    ++iter;
  }
}

}  // namespace bustub
