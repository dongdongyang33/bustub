//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetNextPageId(INVALID_PAGE_ID);
    LOG_INFO("[leaf page init] init done. page_id = %d, parent_page_id = %d, max_size = %d\n", 
                                                    GetPageId(), GetParentPageId(), max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { 
    return next_page_id_; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
    int l = 0, r = GetSize() - 1;
    int ret = GetSize();
    while (l <= r) {
        int m = l + ( r - l ) / 2;
        //LOG_INFO("[leaf-KeyIndex] mid = %d.", m);
        int cmp = comparator(array[m].first, key);
        if (cmp >= 0) {
            ret = m;
            if (cmp == 0) {
                //LOG_INFO("[leaf-KeyIndex] mid.key == key, break.");
                break;
            } else {
                //LOG_INFO("[leaf-KeyIndex] mid.key > key, go to left section.");
                r = m - 1;
            }
        } else {
            //LOG_INFO("[leaf-KeyIndex] mid.key < key, go to right section.");
            l = m + 1;
        }
    }
    if (ret == GetSize()) {LOG_INFO("[leaf-KeyIndex] array[i].first >= key no exist. return the current size.");}
    return ret; 
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
    int current_size = GetSize();
    assert(current_size <= GetMaxSize());
    int position = KeyIndex(key, comparator);
    //assert(comparator(array[position], key) <= 0);
    //if (position == -1) position = 0;
    LOG_INFO("[leaf-Insert] insert position %d\n", position);
    for (int i = current_size; i > position; i--) {
        array[i] = array[i-1];
    }
    array[position].first = key;
    array[position].second = value;
    
    current_size += 1;
    SetSize(current_size);
    return current_size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient,
                                            __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    int current_size = GetSize();
    assert(current_size > GetMaxSize());
    int left_half = (current_size + 1) / 2;
    recipient->CopyNFrom(array + left_half, current_size - left_half);
    SetSize(left_half);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
    int current_size = GetSize();
    int new_size = current_size + size;
    assert(new_size <= GetMaxSize());
    for (int i = 0; i < size; i++) {
        array[current_size + i] = items[i];
    }
    SetSize(new_size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
    bool ret = false;
    int location = LookUpTheKey(key, comparator);
    if (location != -1) {
        ret = true;
        *value = array[location].second;
    }
    return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) { 
    int current_size = GetSize();
    int position =  LookUpTheKey(key, comparator);
    if (position != -1) {
        current_size -= 1;
        for(int i = position; i < current_size; i++){
            array[i] = array[i+1];
        }
        SetSize(current_size);
        LOG_INFO("[leaf-Remove] remove position %d. current array size is %d\n", position, current_size);
    } else {
        LOG_INFO("[leaf-Remove] didn't find match key.\n");
    }
    return current_size; 
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, 
                                            __attribute__((unused)) const KeyType &middle_key, 
                                            __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    recipient->CopyNFrom(array, GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                                            __attribute__((unused)) const KeyType &middle_key, 
                                            __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    int current_size = GetSize();
    assert(current_size > GetMinSize());
    recipient->CopyLastFrom(array[0]);
    current_size -= 1;
    for(int i = 0; i < current_size; i++) {
        array[i] = array[i+1];
    }
    SetSize(current_size);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    int size = GetSize();
    assert(size < GetMinSize());
    array[size] = item;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,
                                            __attribute__((unused)) const KeyType &middle_key, 
                                            __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    assert(size > GetMinSize());
    size -= 1;
    recipient->CopyFirstFrom(array[size]);
    SetSize(size);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
    assert(GetSize() < GetMaxSize() - 1);
    for(int i = GetSize(); i > 0; i--) {
        array[i] = array[i-1];
    }
    array[0] = item;
    IncreaseSize(1);
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::LookUpTheKey(const KeyType &key, const KeyComparator &comparator) const {
    int l = 0, r = GetSize() - 1;
    while (l <= r) {
        int m = l + ( r - l ) / 2;
        //LOG_INFO("[leaf-lookup] mid = %d.", m);
        int cmp = comparator(array[m].first, key);
        if (cmp == 0) {
            //LOG_INFO("[leaf-lookup] mid.key == key, return.");
            return m;
        } else {
            if (cmp > 0) {
                //LOG_INFO("[leaf-lookup] mid.key > key, go to left section.");
                r = m - 1;
            }
            else {
                //LOG_INFO("[leaf-lookup] mid.key < key, go to right section.");
                l = m + 1;
            }
        }
    }
    LOG_INFO("[leaf-lookup] did not find the needed key, return.");
    return -1;
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
