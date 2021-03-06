/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"
#include "common/logger.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page* page, int idx, BufferPoolManager* _bpm) {
    current_page = page;
    current_index = idx;
    bpm = _bpm; 
    if (page != nullptr) {
        B_PLUS_TREE_LEAF_PAGE_TYPE* opt_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(current_page->GetData());
        LOG_INFO("[iterator] init done. page id = %d, index = %d\n", opt_page->GetPageId(), current_index);
    } else {
        LOG_INFO("[iterator] init End() iterator with index %d.", current_index);
    }
} 


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    if(current_page != nullptr) {
        LOG_INFO("[~iterator] unlatch and unpin the page from bmp.\n");
        current_page->RUnlatch();
        bpm->UnpinPage(current_page->GetPageId(), false);
    }    
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { 
    if (current_page == nullptr && current_index == -1) return true;
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 
    assert(current_page != nullptr);
    B_PLUS_TREE_LEAF_PAGE_TYPE* opt_page = 
        reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*> (current_page->GetData());
    return opt_page->GetItem(current_index);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() { 
    if (current_page != nullptr) {
        B_PLUS_TREE_LEAF_PAGE_TYPE* opt_page =
            reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(current_page->GetData());
        if (current_index < opt_page->GetSize() - 1) {
            current_index++;
            LOG_INFO("[iterator++] inside the current page %d, current index = %d\n", 
                                                opt_page->GetPageId(), current_index);
        } else {
            LOG_INFO("[iterator++] current page id = %d ", opt_page->GetPageId());
            page_id_t next_page_id = opt_page->GetNextPageId();
            LOG_INFO("next page id = %d\n", next_page_id);
            if(next_page_id == INVALID_PAGE_ID) {
                LOG_INFO("[iterator++] end of all page.\n");
                current_page->RUnlatch();
                current_page = nullptr;
                current_index = -1;
            } else {
                LOG_INFO("[iterator++] go to the next page.\n");
                Page* next_page = bpm->FetchPage(next_page_id);
                if (next_page != nullptr) {
                    next_page->RLatch();
                    current_page = next_page;
                    current_index = 0;
                } else {
                    throw std::runtime_error("bufferpoolmanager full while operator++");
                }
            }
            bpm->UnpinPage(opt_page->GetPageId(), false, LatchType::READ);
        }
    }
	
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
    return (current_page == itr.current_page && current_index == itr.current_index );
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
    return (current_page != itr.current_page || current_index != itr.current_index);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
