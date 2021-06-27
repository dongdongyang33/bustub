//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    treelatch.RLock();
    bool ret = true;
    if(root_page_id_ != nullptr) ret = false;
    treelatch.RUnlock();
    return ret;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
    bool ret = false;
    Page* page = GetLeafPageOptimistic(true, key);
    if (page != nullptr) {
        BPlusTreeLeafPage* opt_page = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
        ValueType value;
        ret = opt_page->LookUp(key, value, comparator_);
        if (ret) result->push_back(value);
    }
    ReleaseLatchAndDeletePage(transaction, true);
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::GetLeafPageOptimistic(bool isRead, const KeyType &key, Transaction* txn) {
    treelatch.RLock();
    txn->SetTreeLatch(true);

    page_id_t current_page_id = root_page_id_;
    Page* ret = nullptr;        
    if (current_page_id == INVALID_PAGE_ID) {
        ReleaseLatchAndDeletePage(txn, true);
        return ret;
    }

    while (current_page_id != INVALID_PAGE_ID) {
        Page* opt = FetchNeedPageFromBPM(current_page_id);
        BPlusTreePage* current_page = reinterpret_cast<BPlusTreePage*>(opt->GetData());
        if ( !current_page->IsLeafPage() ) {
            opt->RLatch();
            BPlusTreeInternalPage* current_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(current_page);
            ValueType next_id = current_internal_page->LookUp(key, comparator_);
            current_page_id = reinterpret_cast<page_id_t>(next_id); 
        } else {
            if(isRead) opt->RLatch();
            else opt->WLatch();
            ret = opt;
            current_page_id = INVALID_PAGE_ID;
        }
        ReleaseLatchAndDeletePage(txn, true);
        txn->AddIntoPageSet(opt);
    }
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::GetLeafPagePessimistic(bool isInsert, const KeyType &key, Transaction* txn) {
    treelatch.WLock();
    txn->SetTreeLatch(true);
    page_id_t current_page_id = root_page_id_;
    Page* ret = nullptr;
    while (current_page_id != INVALID_PAGE_ID) {
        Page* opt = FetchNeedPageFromBPM(current_page_id);
        opt->WLatch();
        BPlusTreePage* current_page = reinterpret_cast<BPlusTreePage*>(opt->GetData());
        bool isSafe = false;
        if (isInsert && current_page->IsSafeToInsert()) isSafe = true;
        if (!isInsert && current_page->IsSafeToRemove()) isSafe = true;
        if (isSafe) ReleaseLatchAndDeletePage(txn, false);
        txn->AddIntoPageSet(opt);
        if (!current_page->IsLeafPage()) {
            BPlusTreeInternalPage* current_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(current_page);
            ValueType next_id = current_internal_page->LookUp(key, comparator_);
            current_page_id = reinterpret_cast<page_id_t>(next_id); 
        } else {
            ret = opt;
            current_page_id = INVALID_PAGE_ID;
        }
    }
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchAndDeletePage(Transaction* txn, bool isRead) {
    if (txn->GetTreeLatch()) {
        txn->SetTreeLatch(false);
        if (isRead) treelatch.RUnlock();
        else treelatch.WUnlock();
    }

    std::shared_ptr<std::deque<Page *>> page_set = txn->GetPageSet();
        while(!page_set->empty()) {
        Page* opt = page_set->front();
        page_set->pop_front();
        buffer_pool_manager_->UnpinPage(opt->GetPageId, false);
        if (isRead) opt->RUnlatch();
        else opt->WUnlatch();
    }

    std::shared_ptr<std::deque<Page *>> release_page_set = txn->GetReleasePageSet();
    while (!release_page_set->empty) {
        Page* opt = release_page_set->front();
        release_page_set->pop_front();
        buffer_pool_manager_->UnpinPage(opt->GetPageId, true);
        opt->WUnlatch();
    }

    std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set = txn->GetDeletedPageSet();
    for (auto it = deleted_page_set.begin(); it != deleted_page_set.end(); it++) {
        buffer_pool_manager_->DeletePage(*it);
    }
    release_page_set->clear();
}


INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::FetchNeedPageFromBPM(page_id_t pid) {
    Page* ret = buffer_pool_manager_->FetchPage(pid);
    if (ret == nullptr) std::runtime_error("[FetchPageError] No free frame in buffer pool manager!");
    else return ret;
}




/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
    bool ret = true, usePessimistic = false;
    Page* opt_page = GetLeafPageOptimistic(false, key, transaction);
    if (opt_page == nullptr) {
        usePessimistic = true;
    } else {
        BPlusTreeLeafPage* leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(opt_page->GetData());
        if(!leaf_page->IsSafeToInsert()){
            usePessimistic = true;
        } 
    }

    if (usePessimistic){
        ReleaseLatchAndDeletePage(txn, false);
        opt_page = GetLeafPagePessimistic(true, key, transaction);
    } 

    if(opt_page == nullptr) {
        StartNewTree(key, value);
    } else {
        ret = InsertIntoLeaf(key, value, transaction);
    }

    ReleaseLatchAndDeletePage(transaction, true);
    return ret; 
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    assert(root_page_id_ == INVALID_PAGE_ID);
    page_id_t root_page_id;
    Page* page = buffer_pool_manager_->NewPage(root_page_id);
    if(page == nullptr) std::runtime_error("[NewPageError] No free frame in buffer pool manager!");
    BPlusTreeLeafPage* opt_page = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
    opt_page.Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
    opt_page.Insert(key, value, comparator_);
    root_page_id_ = root_page_id;
    buffer_pool_manager_->UnpinPage(root_page_id);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
    std::shared_ptr<std::deque<Page *>> opt_page_set = txn->GetPageSet();
    std::shared_ptr<std::deque<Page *>> opt_release_page_set = txn->GetReleasePageSet();
    Page* opt = opt_page_set->front();
    opt_page_set->pop_back();
    opt_release_page_set->push_back(opt);   

    BPlusTreeLeafPage* leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(opt->GetData());
    bool ret = !(leaf_page->Lookup(key, value, comparator_));
    if (!ret) {
        if (leaf_page->Insert(key, value, comparator_) >= leaf_page->GetMaxSize()) {
            BPlusTreeLeafPage* new_page = Split(leaf_page);
            if (leaf_page->IsRootPage()) {

            }
            InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction);
        }
    }
    return ret;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t page_id;
    Page* new_page = buffer_pool_manager_->NewPage(page_id);
    if (new_page == nullptr) std::runtime_error("[Split-NewPageError] No free frame in buffer pool manager!");
    
    N* new_node = reinterpret_cast<N*>(new_page->GetData());
    int init_size = new_node->IsLeafPage() ? leaf_max_size_ : internal_max_size_;
    new_node->Init(page_id, node->GetParentId(), init_size);
    node->MoveHalfTo(new_node);
    
    if (node->IsRootPage()) {
        page_id_t new_root_id;
        Page* new_root_page = buffer_pool_manager_->NewPage(new_root_id);
        if (new_page == nullptr) std::runtime_error("[Split-NewRootPageError] No free frame in buffer pool manager!");
        
        BPlusTreeInternalPage* root_node = reinterpret_cast<BPlusTreeInternalPage*>(new_root_page->GetData());
        root_node->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
        root_node->PopulateNewRoot(node->GetPageId(), new_node->KeyAt(0), page_id);
        
        node->SetParentPageId(new_root_id);
        new_node->SetParentPageId(new_root_id);

        root_page_id_ = new_root_id;
    }

    return new_node;
}


/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
    std::shared_ptr<std::deque<Page *>> opt_page_set = txn->GetPageSet();
    std::shared_ptr<std::deque<Page *>> opt_release_page_set = txn->GetReleasePageSet();
    Page* opt = opt_page_set->front();
    opt_release_page_set->push_back(opt);
    opt_page_set->pop_back();

    BPlusTreeInternalPage* internal_page = reinterpret_cast<BPlusTreeInternalPage*>(opt->GetData());
    bool needSplit = false;
    ValueType old_value;
    if (old_node->IsLeafPage()) {
        BPlusTreeLeafPage* old_node_leaf = reinterpret_cast<BPlusTreeLeafPage*>(old_node);
        old_value = old_node_leaf->KeyAt(0);
    } else {
        BPlusTreeInternalPage* old_node_internal = reinterpret_cast<BPlusTreeInternalPage*>(old_node);
        old_value = old_node_internal->KeyAt(0);
    }
    if (internal_page->InsertNodeAfter(old_value, key, new_node->GetPageId()) >= internal_page->GetMaxSize()) needSplit = true;

    if (needSplit) {
        BPlusTreeInternalPage* new_page = Split(internal_page);
        InsertIntoParent(internal_page, new_page->KeyAt(0), new_page, transaction);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    Page* opt_page = GetLeafPageOptimistic(false, key, transaction);
    if (opt_page != nullptr) {
        BPlusTreeLeafPage* tree_page = reinterpret_cast<BPlusTreeLeafPage*>(opt_page->GetData());
        bool getAgain = false;
        if (!tree_page->IsSafeToRemove()) {
            ReleaseLatchAndDeletePage(transaction, false);
            opt_page = GetLeafPagePessimistic(false, key, transaction);
            if (opt_page != INVALID_PAGE_ID) {
                tree_page = reinterpret_cast<BPlusTreeLeafPage*>(opt_page->GetData());
                getAgain = true;
            }
        }

        if (getAgain) {
            if (tree_page->RemoveAndDeleteRecord(key, comparator_) < tree_page->GetMinSize()) {
                CoalesceOrRedistribute(tree_page, transaction);
            }
            ReleaseLatchAndDeletePage(transaction, false);
        }
    }
    return;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    assert(node->GetSize() == node->GetMinSize() - 1);
    bool ret = false;
    std::shared_ptr<std::deque<Page*>> page_set = transaction->GetPageSet();
    std::shared_ptr<std::deque<Page*>> release_page_set = transaction->GetReleasePageSet();

    if (node->IsRootPage()){
        std::shared_ptr<std::deque<Page*>> delete_page_set = transaction->GetDeletedPageSet();
        Page* current_page = page_set->front();
        page_set->pop_front();
        delete_page_set->push_back(current_page);
        AdjustRoot(node);
    } else {
        Page* current_page = page_set->front();
        page_set->pop_front();

        Page* parent_page = page_set->front();
        BPlusTreeInternalPage* parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
        int current_index = parent_page->ValueIndex(node->GetPageId());
        int sibling_index = (current_index == 0) ? 1 : current_index - 1;
        page_id_t sibling_page_id = parent_node->ValueAt(sibling_index);
        Page* sibling_page = FetchNeedPageFromBPM(sibling_page_id);
        sibling_page->WLatch();

        N* sibling_node = reinterpret_cast<N*>(sibling_page->GetData());
        if (node->GetSize() + sibling_node->GetSize() < node->GetMaxSize()) {
            // coalesce: merge right to left and delete right page.
            std::shared_ptr<std::deque<Page*>> delete_page_set = transaction->GetDeletedPageSet();
            bool delete_parent;
            if (current_index < sibling_index){
                delete_parent = Coalesce(sibling_index, node, parent_node, sibling_index, transaction);
                release_page_set->push_back(current_page);
                delete_page_set->push_back(sibling_page);
            } else {
                delete_parent = Coalesce(node, sibling_node, parent_node, current_index, transaction);
                release_page_set->push_back(sibling_page);
                delete_page_set->push_back(current_page);
            }
            if (delete_parent) ret = CoalesceOrRedistribute(parent_node, transaction);
        } else {
            // redistribution
            Redistribute(sibling_node, node, current_index);
            release_page_set->push_back(current_page);
            release_page_set->push_back(sibling_page);
        }
    }
    return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion => ???
 * @return  true means parent size < min size, should call CoalesceOrRedistribute() again, while false not.
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
    bool ret = false;
    node->MoveAllTo(neighbor_node);
    parent->Remove(index);
    if (parent->GetSize() < parent->GetMinSize()) ret = true; 
    return ret;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    if(index == 0) {
        neighbor_node->MoveFirstToEndOf(node);
    } else {
        neighbor_node->MoveLastToFrontOf(node);
    }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { 
    if (old_root_node->IsLeafPage()) {
        assert(old_root_node->GetSize() == 0);
        root_page_id_ = INVALID_PAGE_ID;
    } else {
        assert(old_root_node->GetSize() == 1);
        BPlusTreeInternalPage* opt = reinterpret_cast<BPlusTreeInternalPage*>(old_root_node);
        page_id_t page_id = opt->RemoveAndReturnOnlyChild();
        root_page_id_ = page_id;
    }
    return true; 
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
    KeyType key;
    Page* page = GetLeafPageOptimisticForIterator(key, -1);
    return INDEXITERATOR_TYPE(page, buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
    Page* page = GetLeafPageOptimisticForIterator(key, 0);
    return INDEXITERATOR_TYPE(page, buffer_pool_manager_); 
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::GetLeafPageOptimisticForIterator(const KeyType &key, int position) {
    // position {-1: leafmost, 0: input key, 1: end}
    treelatch.RLock();

    page_id_t current_page_id = root_page_id_;
    Page* ret = nullptr;
    if (current_page_id == INVALID_PAGE_ID) {
        treelatch.RUnlock();
        return ret;
    }

    Page* pre_page = nullptr;
    page_id_t unpin;
    while (current_page_id != INVALID_PAGE_ID) {
        Page* opt = FetchNeedPageFromBPM(current_page_id);
        page_id_t save_for_unpin = current_page_id;
        BPlusTreePage* current_page = reinterpret_cast<BPlusTreePage*>(opt->GetData());
        opt->RLatch();
        if ( !current_page->IsLeafPage() ) {
            BPlusTreeInternalPage* current_internal_page = reinterpret_cast<BPlusTreeInternalPage*>(current_page);
            ValueType next_id;
            if (position == 0) {
                next_id = current_internal_page->LookUp(key, comparator_);
            } else {
                int need_inde = (position == 1) ? current_internal_page->GetSize() - 1 : 0;
                next_id = current_internal_page->ValueAt(need_inde);
            }
            current_page_id = reinterpret_cast<page_id_t>(next_id); 
        } else {
            ret = opt;
            current_page_id = INVALID_PAGE_ID;
        }
        if (pre_page == nullptr) {
            treelatch.RUnlock();
        } else {
            pre_page->RUnlatch();
            buffer_pool_manager_->UnpinPage(unpin);
        }
        pre_page = opt;
        unpin = save_for_unpin;
    }
    return ret;
}


/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
    KeyType key;
    Page* page = GetLeafPageOptimisticForIterator(key, 1);
    return INDEXITERATOR_TYPE(page, buffer_pool_manager_); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
    int position = leftMost ? -1 : 0;
    Page* ret = GetLeafPageOptimisticForIterator(key, position);
    return ret;
    //throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
