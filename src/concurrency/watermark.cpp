#include "concurrency/watermark.h"
#include <algorithm>
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  // Insert the read timestamp
  current_reads_.insert(read_ts);

  // Update watermark
  watermark_ = *current_reads_.begin();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  // Remove the timestamp
  auto it = current_reads_.find(read_ts);
  if (it == current_reads_.end()) {
    throw Exception("Attempting to remove non-existing read timestamp");
  }

  current_reads_.erase(it);

  // Recompute watermark if needed
  if (current_reads_.empty()) {
    watermark_ = commit_ts_;
  } else {
    watermark_ = *current_reads_.begin();
  }
}

}  // namespace bustub
