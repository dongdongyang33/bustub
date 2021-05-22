//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    maxsize = num_pages;
}


LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> lock(mtx);
    bool ret = false;
    if (mapping.size() != 0) {
        frame_id_t fid = lru.back();
        lru.pop_back();
        mapping.erase(fid);
        *frame_id = fid;
        ret = true;
    }
    return ret; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = mapping.find(frame_id);
    if (it != mapping.end()) {
        lru.erase(it->second);
        mapping.erase(frame_id);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mtx);
    // assert(mapping.size() < maxsize);
    auto it = mapping.find(frame_id);
    if (it != mapping.end()) {
        lru.erase(it->second);
    }
    lru.push_front(frame_id);
    mapping[frame_id] = lru.begin();
}

size_t LRUReplacer::Size() { 
    std::lock_guard<std::mutex> lock(mtx);
    return mapping.size(); 
}

}  // namespace bustub
