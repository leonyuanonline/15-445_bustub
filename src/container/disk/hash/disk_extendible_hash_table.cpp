//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // 创建并初始化头页面
  // LOG_DEBUG the bpm info
  LOG_DEBUG("BufferPoolManager info: pool_size %lu", bpm_->GetPoolSize());

  LOG_DEBUG("header max depth %d dir max dtpth %d bucket max size %d", header_max_depth, directory_max_depth,
            bucket_max_size);
  page_id_t new_header_page_id;
  auto header_page_guard = bpm_->NewPageGuarded(&new_header_page_id);
  auto *header_page = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page_guard.GetDataMut());

  // 设置头页面的最大深度
  header_page_id_ = new_header_page_id;
  header_page->Init(header_max_depth_);

  // 初始化目录页
  page_id_t new_dir_page_id;
  auto dir_page_guard = bpm_->NewPageGuarded(&new_dir_page_id);
  auto *directory_page = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_page_guard.GetDataMut());

  directory_page->Init(directory_max_depth_);

  // 将页面刷入磁盘
  bpm_->UnpinPage(header_page_guard.PageId(), true);
  bpm_->UnpinPage(dir_page_guard.PageId(), true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result,
                                                 Transaction *transaction) const -> bool {
  uint32_t hash = Hash(key);

  // fetch header page
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *header_page = reinterpret_cast<const ExtendibleHTableHeaderPage *>(header_page_guard.GetData());

  // fetch dir page
  auto dir_idx = header_page->HashToDirectoryIndex(hash);
  auto dir_guard = bpm_->FetchPageRead(header_page->GetDirectoryPageId(dir_idx));
  if (!dir_guard.Isvalid()) {
    return false;
  }
  auto *directory = reinterpret_cast<const ExtendibleHTableDirectoryPage *>(dir_guard.GetData());

  // fetch bucket page
  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_guard = bpm_->FetchPageRead(directory->GetBucketPageId(bucket_idx));
  auto *bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetData());

  LOG_DEBUG("bucket idx %d size %d", bucket_idx, bucket->Size());
  for (uint32_t i = 0; i < bucket->Size(); i++) {
    if (cmp_(bucket->KeyAt(i), key) == 0) {
      result->push_back(bucket->ValueAt(i));
      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  // bool header_dirty = false;
  LOG_DEBUG("Computed hash: %u", hash);

  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page_guard.GetDataMut());

  auto dir_idx = header_page->HashToDirectoryIndex(hash);
  LOG_DEBUG("dir_idx %d, max_size %d", dir_idx, header_page->MaxSize());

  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_idx);

  // 如果目录页面 ID 未初始化，则创建新的目录页面
  if (dir_page_id == INVALID_PAGE_ID) {
    page_id_t new_dir_page_id;
    auto new_dir_guard = bpm_->NewPageGuarded(&new_dir_page_id);
    auto *new_directory = reinterpret_cast<ExtendibleHTableDirectoryPage *>(new_dir_guard.GetDataMut());

    new_directory->Init(directory_max_depth_);
    header_page->SetDirectoryPageId(dir_idx, new_dir_page_id);
    dir_page_id = new_dir_page_id;

    // Unpin the new directory page
    // header_dirty = true;
    bpm_->UnpinPage(new_dir_page_id, true);
  }
  // bpm_->UnpinPage(header_page_id_, header_dirty);
  auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto *directory = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_guard.GetDataMut());
  header_page_guard.Drop();

  while (true) {
    auto bucket_idx = directory->HashToBucketIndex(hash);
    LOG_DEBUG("bucket_idx %d", bucket_idx);

    page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

    // 如果桶页面 ID 未初始化，则创建新的桶页面
    if (bucket_page_id == INVALID_PAGE_ID) {
      page_id_t new_bucket_page_id = INVALID_PAGE_ID;
      auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
      // if (new_bucket_page_id == INVALID_PAGE_ID) {
      //   return false;
      // }
      auto *new_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(new_bucket_guard.GetDataMut());

      new_bucket->Init(bucket_max_size_);
      directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
      bucket_page_id = new_bucket_page_id;

      // Unpin the new bucket page
      bpm_->UnpinPage(new_bucket_page_id, true);
    }

    auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto *bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());

    // If the key already exists, return false
    V tmp_val;
    if (bucket->Lookup(key, tmp_val, cmp_)) {
      return false;
    }

    // If the bucket is full, split the bucket
    if (bucket->IsFull()) {
      LOG_DEBUG("bucket is full");
      if (!Split(directory, bucket_idx, std::move(bucket_guard))) {
        return false;
      }
      continue;
    }

    // Otherwise, insert into the bucket
    bucket->Insert(key, value, cmp_);
    return true;
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Split(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                              WritePageGuard &&old_bucket_guard) -> bool {
  auto *old_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(old_bucket_guard.GetDataMut());
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  LOG_DEBUG("get old bucket");
  // 增加局部深度
  uint32_t updated_local_depth = local_depth + 1;

  // 检查是否需要增加全局深度
  if (updated_local_depth > directory->GetGlobalDepth()) {
    if (!directory->IncrGlobalDepth()) {
      return false;
    }
  }

  // 创建新的桶，并获取其页面ID
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  LOG_DEBUG("new bucket page id %d", new_bucket_page_id);
  // if (new_bucket_page_id == INVALID_PAGE_ID) {
  //   return false;
  // }
  auto *new_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(new_bucket_guard.GetDataMut());
  new_bucket->Init(bucket_max_size_);

  // 更新目录中的桶映射和局部深度
  directory->SetLocalDepth(bucket_idx, updated_local_depth);
  uint32_t new_bucket_idx = bucket_idx ^ (1 << local_depth);
  directory->SetLocalDepth(new_bucket_idx, updated_local_depth);
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);

  std::cout << "Splitting bucket, old bucket size: " << old_bucket->Size() << ", local depth: " << local_depth
            << std::endl;
  std::cout << "New bucket page id: " << new_bucket_page_id << ", local depth: " << updated_local_depth << std::endl;

  std::vector<std::pair<K, V>> items_to_remove;
  for (uint32_t i = 0; i < old_bucket->Size(); i++) {
    K existing_key = old_bucket->KeyAt(i);
    V existing_value = old_bucket->ValueAt(i);

    uint32_t existing_hash = Hash(existing_key);
    bool move_to_new_bucket = new_bucket_idx == directory->HashToBucketIndex(existing_hash);

    if (move_to_new_bucket) {
      if (new_bucket->Insert(existing_key, existing_value, cmp_)) {
        items_to_remove.emplace_back(existing_key, existing_value);  // 记录需要从旧桶中删除的项
      } else {
        std::cerr << "Failed to insert into new bucket. Key: " << existing_key << std::endl;
      }
    }
  }

  // 从旧桶中删除被移至新桶的项
  for (const auto &item : items_to_remove) {
    old_bucket->Remove(item.first, cmp_);
  }

  std::cout << "After rehashing, old bucket size: " << old_bucket->Size() << ", new bucket size: " << new_bucket->Size()
            << std::endl;
  UpdateDirectoryMapping(directory, new_bucket_idx, new_bucket_page_id, updated_local_depth,
                         directory->GetLocalDepthMask(new_bucket_idx));
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  header_max_depth_++;
  for (uint32_t i = 0; i < (1U << header_max_depth_); i++) {
    page_id_t dir_page_id = header->GetDirectoryPageId(i);
    header->SetDirectoryPageId(i + (1 << header_max_depth_), dir_page_id);
  }

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  auto new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);

  auto *new_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(new_bucket_guard.GetDataMut());

  new_bucket->Insert(key, value, cmp_);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  for (uint32_t i = 0; i < directory->Size(); i++) {
    if ((i & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      directory->SetBucketPageId(i, new_bucket_page_id);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  // 获取目录页并确定对应的桶
  std::cout << "remove " << key << std::endl;

  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page_guard.GetDataMut());

  auto dir_idx = header_page->HashToDirectoryIndex(hash);
  auto dir_page_id = header_page->GetDirectoryPageId(dir_idx);

  auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
  if (!dir_guard.Isvalid()) {
    LOG_DEBUG("Failed to fetch directory page with id %d", dir_page_id);
    return false;
  }
  auto *directory = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_guard.GetDataMut());
  header_page_guard.Drop();

  auto bucket_page_id = directory->GetBucketPageId(directory->HashToBucketIndex(hash));
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto *bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());

  // 如果桶中存在这个键值对，删除它
  if (bucket->Remove(key, cmp_)) {
    // 检查桶是否为空，并尝试合并
    if (bucket->IsEmpty()) {
      MergeBuckets(directory, std::move(bucket_guard), directory->HashToBucketIndex(hash));
    }
    return true;
  }

  return false;
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBuckets(ExtendibleHTableDirectoryPage *directory,
                                                     WritePageGuard &&bucket_guard, uint32_t bucket_idx) {
  auto *bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_guard.GetDataMut());
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);

  while (local_depth > 0) {
    // 计算分裂映像的索引
    uint32_t split_image_idx = bucket_idx ^ (1 << (local_depth - 1));
    page_id_t split_image_page_id = directory->GetBucketPageId(split_image_idx);
    auto split_image_guard = bpm_->FetchPageWrite(split_image_page_id);
    auto *split_image_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(split_image_guard.GetDataMut());

    // 检查是否需要进一步合并
    if (!bucket->IsEmpty() && !split_image_bucket->IsEmpty()) {
      break;
    }
    // 如果分裂映像桶的局部深度相同，则尝试合并
    if (directory->GetLocalDepth(split_image_idx) == local_depth) {
      // 合并后更新目录
      LOG_DEBUG("bucket idx %d, split image idx %d bucket is empty %d split image is empty %d", bucket_idx,
                split_image_idx, bucket->IsEmpty(), split_image_bucket->IsEmpty());
      if (bucket->IsEmpty()) {
        directory->SetBucketPageId(bucket_idx, directory->GetBucketPageId(split_image_idx));
        bucket_idx = split_image_idx;
        bucket_guard = std::move(split_image_guard);
        bucket = split_image_bucket;
        LOG_DEBUG("bucket idx %d, bucket is empty %d", bucket_idx, bucket->IsEmpty());
        UpdateDirectoryMapping(directory, bucket_idx, directory->GetBucketPageId(bucket_idx), local_depth - 1,
                               directory->GetLocalDepthMask(bucket_idx));
      } else {
        directory->SetBucketPageId(split_image_idx, directory->GetBucketPageId(bucket_idx));
        UpdateDirectoryMapping(directory, split_image_idx, directory->GetBucketPageId(split_image_idx), local_depth - 1,
                               directory->GetLocalDepthMask(split_image_idx));
      }
      directory->SetLocalDepth(bucket_idx, local_depth - 1);
      directory->SetLocalDepth(split_image_idx, local_depth - 1);
      LOG_DEBUG("bucket idx %d, buaket page id %d", bucket_idx, directory->GetBucketPageId(bucket_idx));

      local_depth--;  // 减小局部深度，继续尝试合并
      PrintHT();

    } else {
      break;  // 如果局部深度不匹配，则无法合并，退出循环
    }
  }
  // 如果所有桶的局部深度都小于全局深度，尝试缩小目录
  while (directory->CanShrink()) {
    printf("local depth: ");
    for (uint32_t i = 0; i < directory->Size(); i++) {
      printf("%u ", directory->GetLocalDepth(i));
    }
    printf("\n");
    printf("global depth: %d\n", directory->GetGlobalDepth());
    directory->Shrink();
    LOG_DEBUG("shrink, global depth %d", directory->GetGlobalDepth());
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
