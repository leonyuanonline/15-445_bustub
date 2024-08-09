//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode() : k_(0), fid_(0) {}
LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}
void LRUKNode::RecordAccess(size_t timestamp) {
  if (history_.size() >= k_) {
    history_.pop_front();
  }
  history_.push_back(timestamp);
}
auto LRUKNode::GetKDistance(size_t cur_timestamp) const -> size_t {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  return cur_timestamp - history_.front();
}
auto LRUKNode::GetEarliestTimestamp() const -> size_t {
  if (history_.empty()) {
    return std::numeric_limits<size_t>::max();
  }
  return history_.front();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// Successful eviction of a frame should decrement the size of replacer and remove the frame's
// access history.
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> latch(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  size_t max_k_distance = 0;
  frame_id_t victim_id = -1;
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();
  frame_id_t lru_victim_id = -1;

  for (const auto &entry : node_store_) {
    const auto &node = entry.second;
    if (node.IsEvictable()) {
      size_t k_distance = node.GetKDistance(current_timestamp_);
      if (k_distance == std::numeric_limits<size_t>::max()) {
        size_t timestamp = node.GetEarliestTimestamp();
        if (timestamp < earliest_timestamp) {
          earliest_timestamp = timestamp;
          lru_victim_id = entry.first;
        }
      } else if (k_distance > max_k_distance) {
        max_k_distance = k_distance;
        victim_id = entry.first;
      }
    }
  }

  if (lru_victim_id != -1) {
    *frame_id = lru_victim_id;
  } else if (victim_id != -1) {
    *frame_id = victim_id;
  } else {
    return false;
  }

  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Invalid frame id");
  }
  std::lock_guard<std::mutex> latch(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(k_, frame_id));
  }
  node_store_[frame_id].RecordAccess(current_timestamp_);
  current_timestamp_++;
}

// Note that size is equal to number of evictable entries.
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Invalid frame id");
  }
  std::lock_guard<std::mutex> latch(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(k_, frame_id));
  }
  auto &node = node_store_[frame_id];
  if (node.IsEvictable() != set_evictable) {
    node.SetEvictable(set_evictable);
    curr_size_ += set_evictable ? 1 : -1;
  }
}

// This function should also decrement replacer's size if removal is successful.
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Invalid frame id");
  }
  std::lock_guard<std::mutex> latch(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (!node_store_.at(frame_id).IsEvictable()) {
    throw std::logic_error("Cannot remove non-evictable frame");
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> latch(latch_);
  return curr_size_;
}

}  // namespace bustub
