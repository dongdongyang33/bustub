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
      root_page_id_(HEADER_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
          LOG_INFO("[BPlusTree] leaf_max_size = %d, internal_max_size = %d.\n", leaf_max_size, internal_max_size);
      }

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    bool ret = false;
    treelatch.RLock();
    if(root_page_id_ == HEADER_PAGE_ID) ret = true;
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
    LOG_INFO("[GetValue] Start.\n");
    bool ret = false;
    Page* page = GetLeafPageOptimistic(true, key, transaction);
    if (page != nullptr) {
        LeafPage* opt_page = reinterpret_cast<LeafPage*>(page->GetData());
        ValueType value;
        ret = opt_page->Lookup(key, &value, comparator_);
        if (ret) {
            LOG_INFO("[GetValue] Get value successfully.\n");
            result->push_back(value);
        } else {
            LOG_INFO("[GetValue] no get needed value.\n");
        }
    }
    ReleaseLatchAndDeletePage(transaction, true);
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::GetLeafPageOptimistic(bool isRead, const KeyType &key, Transaction* txn) {
    LOG_INFO("[GetLeafPageOptimistic] start.\n");
    treelatch.RLock();
    txn->SetTreeLatch(true);

    page_id_t current_page_id = root_page_id_;
    Page* ret = nullptr;
    if (current_page_id == HEADER_PAGE_ID) {
        LOG_INFO("[GetLeafPageOptimistic] empty tree. release and return.\n");
        ReleaseLatchAndDeletePage(txn, true);
        return ret;
    }

    while (current_page_id != HEADER_PAGE_ID) {
        //LOG_INFO("[GetLeafPageOptimistic] current page id = %d\n", current_page_id);
        Page* opt = FetchNeedPageFromBPM(current_page_id);
        BPlusTreePage* current_page = reinterpret_cast<BPlusTreePage*>(opt->GetData());
        if ( !current_page->IsLeafPage() ) {
            //LOG_INFO("[GetLeafPageOptimistic] internal page.\n");
            opt->RLatch();
            InternalPage* current_internal_page = reinterpret_cast<InternalPage*>(current_page);
            current_page_id = current_internal_page->Lookup(key, comparator_);
            LOG_INFO("[GetLeafPageOptimistic] internal. current page id = %d\n", current_page_id);
        } else {
            LOG_INFO("[GetLeafPageOptimistic] Get page successfully. Page id = %d\n", current_page_id);
            if(isRead) opt->RLatch();
            else opt->WLatch();
            ret = opt;
            current_page_id = HEADER_PAGE_ID;
        }
        ReleaseLatchAndDeletePage(txn, true);
        txn->AddIntoPageSet(opt);
    }
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::GetLeafPagePessimistic(bool isInsert, const KeyType &key, Transaction* txn) {
    LOG_INFO("[GetLeafPagePessimistic] start.\n");
    treelatch.WLock();
    txn->SetTreeLatch(true);
    page_id_t current_page_id = root_page_id_;
    Page* ret = nullptr;
    while (current_page_id != HEADER_PAGE_ID) {
        Page* opt = FetchNeedPageFromBPM(current_page_id);
        opt->WLatch();
        BPlusTreePage* current_page = reinterpret_cast<BPlusTreePage*>(opt->GetData());
        bool isSafe = false;
        if (isInsert && current_page->IsSafeToInsert()) isSafe = true;
        if (!isInsert && current_page->IsSafeToRemove()) isSafe = true;
        if (isSafe) {
            LOG_INFO("[GetLeafPagePessimistic] safe. release current held lock.\n");
            ReleaseLatchAndDeletePage(txn, false);
        }
        txn->AddIntoPageSet(opt);
        if (!current_page->IsLeafPage()) {
            InternalPage* current_internal_page = reinterpret_cast<InternalPage*>(current_page);
            current_page_id = current_internal_page->Lookup(key, comparator_);
            LOG_INFO("[GetLeafPagePessimistic] internal. current page id = %d\n", current_page_id);
            //page_id_t next_id = current_internal_page->Lookup(key, comparator_);
            //current_page_id = next_id;
            //ValueType next_id = current_internal_page->LookUp(key, comparator_);
            //current_page_id = reinterpret_cast<page_id_t>(next_id); 
        } else {
            LOG_INFO("[GetLeafPagePessimistic] Get page successfully. Page id = %d\n", current_page_id);
            ret = opt;
            current_page_id = HEADER_PAGE_ID;
        }
    }
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatchAndDeletePage(Transaction* txn, bool isRead) {
    LOG_INFO("[ReleaseLatchAndDeletePage] start to release.\n");
    if (txn->GetTreeLatch()) {
        LOG_INFO("[ReleaseLatchAndDeletePage] had tree latch. release tree latch.\n");
        if (isRead) treelatch.RUnlock();
        else treelatch.WUnlock();
        txn->SetTreeLatch(false);
    }

    //LOG_INFO("[ReleaseLatchAndDeletePage] release PageSet.\n");
    std::shared_ptr<std::deque<Page *>> page_set = txn->GetPageSet();
    while(!page_set->empty()) {
        Page* opt = page_set->front();
        //LOG_INFO("[ReleaseLatchAndDeletePage] release PageSet - unlock.\n");
        if (isRead) opt->RUnlatch();
        else opt->WUnlatch();
        //LOG_INFO("[ReleaseLatchAndDeletePage] release PageSet - unpin.\n");
        buffer_pool_manager_->UnpinPage(opt->GetPageId(), false);
        page_set->pop_front();
    }
    page_set->clear();

    //LOG_INFO("[ReleaseLatchAndDeletePage] release ReleasePageSet.\n");
    std::shared_ptr<std::deque<Page *>> release_page_set = txn->GetReleasePageSet();
    while (!release_page_set->empty()) {
        Page* opt = release_page_set->front();
        //LOG_INFO("[ReleaseLatchAndDeletePage] release ReleasePageSet - unlock.\n");
        opt->WUnlatch();
        //LOG_INFO("[ReleaseLatchAndDeletePage] release ReleasePageSet - unpin.\n");
        buffer_pool_manager_->UnpinPage(opt->GetPageId(), true);
        release_page_set->pop_front();
    }
    release_page_set->clear();

    //LOG_INFO("[ReleaseLatchAndDeletePage] delete page in  DeletePageSet.\n");
    std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set = txn->GetDeletedPageSet();
    for (auto it = deleted_page_set->begin(); it != deleted_page_set->end(); it++) {
        buffer_pool_manager_->DeletePage(*it);
    }
    release_page_set->clear();
}


INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::FetchNeedPageFromBPM(page_id_t pid) {
    //LOG_INFO("Fetch Page %d from buffer pool manager.\n", pid);
    Page* ret = buffer_pool_manager_->FetchPage(pid);
    if (ret == nullptr) std::runtime_error("[FetchPageError] No free frame in buffer pool manager!");
    LOG_INFO("[FetchNeedPageFromBPM] Fetch Page %d from buffer pool manager successfully.\n", pid);
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::NewPageFromBPM(page_id_t& pid) {
    //LOG_INFO("[NewPageFromBPM] New a page from buffer pool manager.\n");
    Page* ret = buffer_pool_manager_->NewPage(&pid);
    if (ret == nullptr) std::runtime_error("[NewPageError] No free frame in buffer pool manager!");
    LOG_INFO("[NewPageFromBPM] New page done. New page id = %d.\n", pid);
    return ret;
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
    LOG_INFO("[Insert] Start.\n");
    bool ret = true, usePessimistic = false;
    Page* opt_page = GetLeafPageOptimistic(false, key, transaction);
    if (opt_page == nullptr) {
        LOG_INFO("[Insert] empty tree.\n");
        usePessimistic = true;
    } else {
        BPlusTreePage* bpt_page = reinterpret_cast<BPlusTreePage*>(opt_page->GetData());
        if (!bpt_page->IsSafeToInsert()) usePessimistic = true;
    }

    if (usePessimistic){
        LOG_INFO("[Insert] usePessimistic = true. re-get the page using pessimistic way.\n");
        ReleaseLatchAndDeletePage(transaction, false);
        opt_page = GetLeafPagePessimistic(true, key, transaction);
    } 

    if(opt_page == nullptr) {
        LOG_INFO("[Insert] start new tree.\n");
        StartNewTree(key, value);
    } else {
        LOG_INFO("[Insert] insert into leaf.\n");
        ret = InsertIntoLeaf(key, value, transaction);
    }

    ReleaseLatchAndDeletePage(transaction, false);
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
    assert(root_page_id_ == HEADER_PAGE_ID);
    LOG_INFO("[StartNewTree] start a new tree.\n");

    page_id_t root_page_id = INVALID_PAGE_ID;
    Page* page = NewPageFromBPM(root_page_id);

    LeafPage* opt_page = reinterpret_cast<LeafPage*>(page->GetData());
    opt_page->Init(root_page_id, HEADER_PAGE_ID, leaf_max_size_);
    opt_page->Insert(key, value, comparator_);
    LOG_INFO("[StartNewTree] init opt_page_id = %d, opt_page_parent_id = %d\n", opt_page->GetPageId(), opt_page->GetParentPageId());
    root_page_id_ = root_page_id;
    LOG_INFO("[StartNewTree] root_page_id = %d, root_page_id_ = %d\n", root_page_id, root_page_id_);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id, true);
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
    std::shared_ptr<std::deque<Page *>> opt_page_set = transaction->GetPageSet();
    Page* opt = transaction->GetFromPageSet();
    transaction->AddIntoReleasePageSet(opt);
    opt_page_set->pop_front();

    LeafPage* leaf_page = reinterpret_cast<LeafPage*>(opt->GetData());
    LOG_INFO("[InsertIntoLeaf] current opt page id = %d.", leaf_page->GetPageId());
    ValueType v;
    bool ret = !(leaf_page->Lookup(key, &v, comparator_));
    if (ret) {
        if (leaf_page->Insert(key, value, comparator_) > leaf_page->GetMaxSize()) {
            LOG_INFO("[InsertIntoLeaf] split.\n");
            LeafPage* new_page = Split(leaf_page, transaction);

            if (leaf_page->IsRootPage()) {
                LOG_INFO("[InsertIntoLeaf] root page. add a new root page.\n");
                NewRootPage(leaf_page, new_page, transaction);
            } else {
                LOG_INFO("[InsertIntoLeaf] insert into parent.\n");
                InsertIntoParent(leaf_page, new_page->KeyAt(0), new_page, transaction);
            }
        }
    }
    return ret;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::NewRootPage(N *left_node, N *right_node, Transaction *txn){
    page_id_t new_root_id;
    Page* new_root_page = NewPageFromBPM(new_root_id);
    LOG_INFO("[NewRootPage] new_root_page = %d\n", new_root_id);

    InternalPage* root_node = reinterpret_cast<InternalPage*>(new_root_page->GetData());
    root_node->Init(new_root_id, HEADER_PAGE_ID, internal_max_size_);
    LOG_INFO("[NewRootPage] [left_id, right_id] = [%d, %d]\n", left_node->GetPageId(), right_node->GetPageId());
    root_node->PopulateNewRoot(left_node->GetPageId(), right_node->KeyAt(0), right_node->GetPageId());
    root_page_id_ = new_root_id;
    left_node->SetParentPageId(root_page_id_);
    right_node->SetParentPageId(root_page_id_);
    LOG_INFO("[NewRootPage] root_page_id_ = %d\n", root_page_id_);
    UpdateRootPageId(); 
    txn->AddIntoReleasePageSet(new_root_page);
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
N *BPLUSTREE_TYPE::Split(N *node, Transaction *txn) {
    page_id_t page_id;
    Page* new_page = NewPageFromBPM(page_id);
    txn->AddIntoReleasePageSet(new_page);
    
    N* new_node = reinterpret_cast<N*>(new_page->GetData());
    int initsize = node->IsLeafPage()? leaf_max_size_ : internal_max_size_;
    new_node->Init(page_id, node->GetParentPageId(), initsize);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    
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
    std::shared_ptr<std::deque<Page *>> opt_page_set = transaction->GetPageSet();
    Page* opt = transaction->GetFromPageSet();
    transaction->AddIntoReleasePageSet(opt);
    opt_page_set->pop_front();

    InternalPage* internal_page = reinterpret_cast<InternalPage*>(opt->GetData());
    LOG_INFO("[InsertIntoParent] current opt page id = %d.\n", internal_page->GetPageId());
    LOG_INFO("[InsertIntoParent] [old_id, new_id] = [%d, %d].\n", old_node->GetPageId(), new_node->GetPageId());
    if (internal_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId()) > internal_page->GetMaxSize()){
        LOG_INFO("[InsertIntoParent] split.\n");
        InternalPage* new_page = Split(internal_page, transaction);
        if (internal_page->IsRootPage()) {
            LOG_INFO("[InsertIntoParent] is root page. add a new root page.\n");
            NewRootPage(internal_page, new_page, transaction);
        } else {
            LOG_INFO("[InsertIntoParent] insert into parent.\n");
            InsertIntoParent(internal_page, new_page->KeyAt(0), new_page, transaction);
        }
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
    LOG_INFO("[Remove] Start.\n");
    Page* opt_page = GetLeafPageOptimistic(false, key, transaction);
    if (opt_page != nullptr) {
        LeafPage* tree_page = reinterpret_cast<LeafPage*>(opt_page->GetData());
        if (!tree_page->IsSafeToRemove()) {
            LOG_INFO("[Remove] Unsafe to remove kv. re-get page in pessimistic way.\n");
            ReleaseLatchAndDeletePage(transaction, false);
            opt_page = GetLeafPagePessimistic(false, key, transaction);
            if (opt_page != nullptr) {
                tree_page = reinterpret_cast<LeafPage*>(opt_page->GetData());
            }
        }

        if (tree_page->RemoveAndDeleteRecord(key, comparator_) < tree_page->GetMinSize()) {
            LOG_INFO("[Remove] size < minSize, need to CoalesceOrRedistribute.\n");
            CoalesceOrRedistribute(tree_page, transaction);
        }
        ReleaseLatchAndDeletePage(transaction, false);
        LOG_INFO("[Remove] remove successfully.\n");
    } else {
        LOG_INFO("[Remove] empty tree. no need to remove.\n");
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
    LOG_INFO("[CoalesceOrRedistribute] start.\n");
    bool ret = false;
    std::shared_ptr<std::deque<Page*>> page_set = transaction->GetPageSet();
    std::shared_ptr<std::deque<Page*>> release_page_set = transaction->GetReleasePageSet();

    Page* current_page = transaction->GetFromPageSet();
    page_set->pop_front();

    if (node->IsRootPage()){
        LOG_INFO("[CoalesceOrRedistribute] root page. need adjust root.\n");
        AdjustRoot(node);
        transaction->AddIntoDeletedPageSet(node->GetPageId());
    } else {
        LOG_INFO("[CoalesceOrRedistribute] not a root page.\n");
        Page* parent_page = transaction->GetFromPageSet();
        InternalPage* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

        int current_index = parent_node->ValueIndex(node->GetPageId());
        int sibling_index = (current_index == 0) ? 1 : current_index - 1;
        LOG_INFO("[CoalesceOrRedistribute] in parent [current_index, sibling_index] = [%d, %d].\n", 
                                                                    current_index, sibling_index);

        page_id_t sibling_page_id = parent_node->ValueAt(sibling_index);
        Page* sibling_page = FetchNeedPageFromBPM(sibling_page_id);
        sibling_page->WLatch();

        N* sibling_node = reinterpret_cast<N*>(sibling_page->GetData());
        LOG_INFO("[CoalesceOrRedistribute] [node_id, sibling_id] = [%d, %d].\n", node->GetPageId(), sibling_node->GetPageId());
        if (node->GetSize() + sibling_node->GetSize() <= node->GetMaxSize()) {
            // coalesce: merge right to left and delete right page.
            LOG_INFO("[CoalesceOrRedistribute] Coalesce.\n");
            bool delete_parent;
            if (current_index < sibling_index){
                LOG_INFO("[CoalesceOrRedistribute] move node to sibling.\n");
                delete_parent = Coalesce(&node, &sibling_node, &parent_node, sibling_index, transaction);
                release_page_set->push_back(current_page);
                transaction->AddIntoDeletedPageSet(sibling_page_id);
            } else {
                LOG_INFO("[CoalesceOrRedistribute] move sibling to node.\n");
                delete_parent = Coalesce(&sibling_node, &node, &parent_node, current_index, transaction);
                release_page_set->push_back(sibling_page);
                transaction->AddIntoDeletedPageSet(node->GetPageId());
            }
            if (delete_parent) {
                LOG_INFO("[CoalesceOrRedistribute] parent size < min size.\n");
                ret = CoalesceOrRedistribute(parent_node, transaction);
            } 
        } else {
            // redistribution
            LOG_INFO("[CoalesceOrRedistribute] Redistribute.\n");
            Redistribute(sibling_node, node, current_index);
            if (current_index == 0) parent_node->SetKeyAt(sibling_index, sibling_node->KeyAt(0));
            else parent_node->SetKeyAt(current_index, node->KeyAt(0));

            release_page_set->push_back(current_page);
            release_page_set->push_back(sibling_page);
        }
        if (parent_node->IsRootPage()) UpdateRootPageId();
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
    LOG_INFO("[Coalesce] start.\n");
    bool ret = false;
    (*node)->MoveAllTo(*neighbor_node, (*node)->KeyAt(0), buffer_pool_manager_);
    (*parent)->Remove(index);
    transaction->AddIntoDeletedPageSet((*node)->GetPageId());
    if ((*parent)->GetSize() < (*parent)->GetMinSize()) ret = true; 
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
    LOG_INFO("[Redistribute] start.\n");
    if(index == 0) {
        LOG_INFO("[Redistribute] index == 0. move sibling first to node's end.\n");
        neighbor_node->MoveFirstToEndOf(node, node->KeyAt(0), buffer_pool_manager_);
    } else {
        LOG_INFO("[Redistribute] index != 0. move sibling last to node's first.\n");
        neighbor_node->MoveLastToFrontOf(node, node->KeyAt(0), buffer_pool_manager_);
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
    LOG_INFO("[AdjustRoot] start.\n");
    LOG_INFO("[AdjustRoot] old page id = %d.\n", old_root_node->GetPageId());
    if (old_root_node->IsLeafPage()) {
        assert(old_root_node->GetSize() == 0);
        LOG_INFO("[AdjustRoot] only a leaf in the tree. remove it.\n");
        root_page_id_ = HEADER_PAGE_ID;
    } else {
        assert(old_root_node->GetSize() == 1);
        LOG_INFO("[AdjustRoot] root is an internal page. remove it.\n");
        InternalPage* opt = reinterpret_cast<InternalPage*>(old_root_node);
        page_id_t page_id = opt->RemoveAndReturnOnlyChild();
        root_page_id_ = page_id;
    }
    UpdateRootPageId();
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
    if (page != nullptr) return INDEXITERATOR_TYPE(page, 0, buffer_pool_manager_);
    else return INDEXITERATOR_TYPE(page, -1, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
    Page* page = GetLeafPageOptimisticForIterator(key, 0);
    LeafPage* opt = reinterpret_cast<LeafPage*>(page->GetData());
    int position = opt->LookUpTheKey(key, comparator_);
    if (position > -1) return INDEXITERATOR_TYPE(page, position, buffer_pool_manager_); 
    else {
        std::runtime_error("[Begin(iterator)] didn't find the key!");
        return INDEXITERATOR_TYPE();
    }
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::GetLeafPageOptimisticForIterator(const KeyType &key, int position) {
    // position {-1: leftmost, 0: input key, 1: end}
    // diff with get page optimistic is this function has no txn. 
    // you should unlatch/unlock the paeg manually.
    treelatch.RLock();

    page_id_t current_page_id = root_page_id_;
    Page* ret = nullptr;
    if (current_page_id == HEADER_PAGE_ID) {
        treelatch.RUnlock();
        return ret;
    }

    Page* pre_page = nullptr;
    page_id_t unpin;
    while (current_page_id != HEADER_PAGE_ID) {
        Page* opt = FetchNeedPageFromBPM(current_page_id);
        page_id_t save_for_unpin = current_page_id;
        BPlusTreePage* current_page = reinterpret_cast<BPlusTreePage*>(opt->GetData());
        opt->RLatch();
        if ( !current_page->IsLeafPage() ) {
            InternalPage* current_internal_page = reinterpret_cast<InternalPage*>(current_page);
            ValueType next_id;
            if (position == 0) {
                current_page_id = current_internal_page->Lookup(key, comparator_);
            } else {
                int need_index = (position == 1) ? current_internal_page->GetSize() - 1 : 0;
                current_page_id = current_internal_page->ValueAt(need_index);
            }
            //current_page_id = reinterpret_cast<page_id_t>(next_id); 
        } else {
            ret = opt;
            current_page_id = HEADER_PAGE_ID;
        }
        if (pre_page == nullptr) {
            treelatch.RUnlock();
        } else {
            pre_page->RUnlatch();
            buffer_pool_manager_->UnpinPage(unpin, false);
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
    LeafPage* opt = reinterpret_cast<LeafPage*>(page->GetData());
    return INDEXITERATOR_TYPE(page, opt->GetSize() - 1, buffer_pool_manager_); 
}


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { 
    return INDEXITERATOR_TYPE(nullptr, -1, buffer_pool_manager_); 
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
void BPLUSTREE_TYPE::UpdateRootPageId() {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (!header_page->InsertRecord(index_name_, root_page_id_))
        header_page->UpdateRecord(index_name_, root_page_id_);
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
