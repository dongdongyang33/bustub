/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page* page, BufferPoolManager* _bpm) {
    current_index = 0;
    current_page = page;
    bpm = _bpm;
} 


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    if(current_page != nullptr) bpm->UnpinPage(current_page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { 
    if (current_page == nullptr) return true;
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
    assert(current_page != nullptr);
	B_PLUS_TREE_LEAF_PAGE_TYPE* opt_page =
		reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(current_page->GetData());
    if (current_index < opt_page->GetSize() - 1) {
        current_index++;
    } else {
        page_id_t next_page_id = opt_page->GetNextPageId();
        bpm->UnpinPage(opt_page->GetPageId(), false);
        if(next_page_id == INVALID_PAGE_ID) current_page = nullptr;
        else {
            Page* next_page = bpm->FetchPage(next_page_id);
            if (next_page != nullptr) {
                current_page = next_page;
                current_index = 0;
            } else {
                throw std::runtime_error("bufferpoolmanager full while operator++");
            }
        }
    }
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
    return (current_page == itr.current_page && current_index == itr.current_index);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
    return (current_page == itr.current_page || current_index == itr.current_index);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
