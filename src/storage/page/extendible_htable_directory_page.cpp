//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(page_id_t page_id, uint32_t max_depth) {
  page_id_ = page_id;
  // 初始化最大深度
  max_depth_ = max_depth;

  // 初始化全局深度为 1
  global_depth_ = 0;

  // 初始化局部深度数组，每个桶的初始局部深度都为 1
  for (uint32_t i = 0; i < (1 << max_depth_); i++) {
    local_depths_[i] = 0;
  }

  // 初始化桶页面 ID 数组，开始时所有桶的页面 ID 设置为无效值
  for (uint32_t i = 0; i < (1 << max_depth_); i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1 << (GetLocalDepth(bucket_idx) - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  LOG_DEBUG("get page id %d global depth", page_id_);
  return global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::IncrGlobalDepth() -> bool {
  if (global_depth_ == max_depth_) {
    return false;
  }
  uint32_t old_size = 1 << global_depth_;
  global_depth_++;

  for (uint32_t i = 0; i < old_size; i++) {
    bucket_page_ids_[i + old_size] = bucket_page_ids_[i];
    local_depths_[i + old_size] = local_depths_[i];
  }
  return true;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

void ExtendibleHTableDirectoryPage::Shrink() {
  if (global_depth_ > 0) {
    global_depth_--;

    uint32_t new_size = 1 << global_depth_;
    for (uint32_t i = 0; i < new_size; i++) {
      uint32_t split_image_idx = i + new_size;
      if (bucket_page_ids_[split_image_idx] == bucket_page_ids_[i]) {
        // 如果两者是指向相同桶的映像，那么我们减少局部深度
        if (local_depths_[i] > 0) {
          local_depths_[i]--;
        }
      }
      // 统一指向一个桶
      bucket_page_ids_[split_image_idx] = bucket_page_ids_[i];
      local_depths_[split_image_idx] = local_depths_[i];
    }
  }
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  uint32_t count = 0;
  for (uint32_t i = 0; i < (1 << global_depth_); i++) {
    if (bucket_page_ids_[i] != INVALID_PAGE_ID) {
      count++;
    }
  }
  return count;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  page_id_t target_page_id = bucket_page_ids_[bucket_idx];

  // 遍历所有目录项
  for (uint32_t i = 0; i < (1U << global_depth_); i++) {
    // 如果当前索引指向的页面 ID 与目标页面 ID 相同，则更新局部深度
    if (bucket_page_ids_[i] == target_page_id) {
      local_depths_[i] = local_depth;
    }
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub
