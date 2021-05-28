//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    std::lock_guard<std::mutex> lock(latch_);
    Page* ret = nullptr;
    if (page_table_.find(page_id) != page_table_.end()) {
        frame_id_t fid = page_table_[page_id];
        pages_[fid].pin_count_++;
        replacer_->Pin(fid);
        ret = pages_ + fid;
    } else {
        ret = GetNewPageFromBPM(false, page_id);
    }
    return ret;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
    std::lock_guard<std::mutex> lock(latch_);
    bool ret = false;
    if (page_table_.find(page_id) != page_table_.end()) {
        frame_id_t fid = page_table_[page_id];
        pages_[fid].pin_count_--;
        if (pages_[fid].pin_count_ == 0) {
            replacer_->Unpin(fid);
            pages_[fid].is_dirty_ |= is_dirty;
            ret = true;
        }
    }
    return ret; 
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
    std::lock_guard<std::mutex> lock(latch_);
    return this->FlushSinglePage(page_id);
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    std::lock_guard<std::mutex> lock(latch_);
    Page* ret = GetNewPageFromBPM(true, INVALID_PAGE_ID);
    if(ret != nullptr) *page_id = ret->page_id_;
    return ret;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    std::lock_guard<std::mutex> lock(latch_);
    bool ret = true;
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t fid = it->second;
        if (pages_[fid].pin_count_ > 0) ret = false;
        else {
            disk_manager_->DeallocatePage(page_id);
            page_table_.erase(it);
            free_list_.push_back(fid);
        }
    }
    return ret;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
    std::lock_guard<std::mutex> lock(latch_);
    for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
        FlushSinglePage(it->first);
    }
}

bool BufferPoolManager::FlushSinglePage(page_id_t page_id) {
    bool ret = false;
    if (page_id != INVALID_PAGE_ID) {
        if( page_table_.find(page_id) != page_table_.end()) {
            frame_id_t fid = page_table_[page_id];
            if (pages_[fid].is_dirty_) disk_manager_->WritePage(page_id, pages_[fid].GetData());
            pages_[fid].is_dirty_ &= 0;
            ret = true;
        }
    }
    return ret;
}

Page* BufferPoolManager::GetNewPageFromBPM(bool newpage, page_id_t page_id) {
    Page* ret = nullptr;
    frame_id_t fid = -1;
    if (!free_list_.empty()) {
        fid = free_list_.front();
        free_list_.pop_front();
    } else {
        if (replacer_->Victim(&fid)) {
            if(pages_[fid].is_dirty_) disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
            pages_[fid].is_dirty_ &= 0;
            page_table_.erase(pages_[fid].page_id_);
            replacer_->Pin(fid);
        }
    }

    if (fid != -1) {
        if (newpage) {
            pages_[fid].ResetMemory();
            pages_[fid].page_id_ = disk_manager_->AllocatePage();
            page_table_[pages_[fid].page_id_] = fid;
        } else {
            pages_[fid].page_id_ = page_id;
            disk_manager_->ReadPage(page_id, pages_[fid].data_);
            page_table_[page_id] = fid;
        }
        pages_[fid].is_dirty_ = false;
        pages_[fid].pin_count_ = 1;
        ret = pages_ + fid;
    }
    
    return ret;
}

}  // namespace bustub
