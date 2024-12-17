//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  if (free_list_.empty() && replacer_->Size() == 0) {
    printf("no free page\n");
    return nullptr;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      printf("evict frame false, no free page\n");
      return nullptr;
    }

    if (pages_[frame_id].is_dirty_) {
      FlushPage(pages_[frame_id].GetPageId());
    }

    page_table_.erase(pages_[frame_id].GetPageId());
  }
  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;

  Page &page = pages_[frame_id];
  page.page_id_ = *page_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page &page = pages_[frame_id];
    page.pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &page;
  }

  // not int the buffer pool
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    if (pages_[frame_id].is_dirty_) {
      FlushPage(pages_[frame_id].GetPageId());
    }

    page_table_.erase(pages_[frame_id].GetPageId());
  }
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest read_request = {false, pages_[frame_id].GetData(), page_id, std::move(promise)};
  disk_scheduler_->Schedule(std::move(read_request));
  future.get();

  // update page info
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  page_table_[page_id] = frame_id;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];

  if (is_dirty) {
    page.is_dirty_ = true;
  }

  if (page.pin_count_ > 0) {
    page.pin_count_--;
  }

  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];
  // std::cout << "flush page " << page_id << std::endl;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest write_r = {true, page.GetData(), page_id, std::move(promise)};
  disk_scheduler_->Schedule(std::move(write_r));
  future.get();
  // std::cout << "flush page " << page_id << " ok" << std::endl;
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (const auto &[page_id, frame_id] : page_table_) {
    FlushPage(page_id);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page &page = pages_[frame_id];

  if (page.pin_count_ > 0) {
    return false;
  }

  replacer_->SetEvictable(frame_id, false);
  if (page.is_dirty_) {
    FlushPage(pages_[frame_id].GetPageId());
  }

  page_table_.erase(page_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  free_list_.emplace_back(frame_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  Page *page = nullptr;

  if (page_id == INVALID_PAGE_ID) {
    return {};
  }

  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  } else {
    if (free_list_.empty() && replacer_->Size() == 0) {
      return {};
    }

    frame_id_t frame_id;
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&frame_id)) {
        return {};
      }

      if (pages_[frame_id].is_dirty_) {
        FlushPage(pages_[frame_id].GetPageId());
      }

      page_table_.erase(pages_[frame_id].GetPageId());
    }

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest read_request = {false, pages_[frame_id].GetData(), page_id, std::move(promise)};
    disk_scheduler_->Schedule(std::move(read_request));
    future.get();

    page = &pages_[frame_id];
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page_table_[page_id] = frame_id;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }

  return {BasicPageGuard(this, page)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  if (page_id == INVALID_PAGE_ID) {
    return {};
  }
  BasicPageGuard basic_guard = FetchPageBasic(page_id);
  if (basic_guard.GetData() == nullptr) {
    return {};
  }
  return basic_guard.UpgradeRead();
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  if (page_id == INVALID_PAGE_ID) {
    return {};
  }
  BasicPageGuard basic_guard = FetchPageBasic(page_id);
  if (basic_guard.GetData() == nullptr) {
    return {};
  }
  return basic_guard.UpgradeWrite();
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  if (page != nullptr) {
    return {BasicPageGuard(this, page)};
  }
  return {};
}

}  // namespace bustub
