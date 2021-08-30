/**
 * b_plus_tree_test.cpp
 */

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <thread>                   // NOLINT
#include "b_plus_tree_test_util.h"  // NOLINT

#include "common/logger.h"
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 15, 8);  
  //BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  //int64_t scale_factor = 10;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(4, InsertHelper, &tree, keys);

  Transaction *transaction_for_get = new Transaction(-1);
  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids, transaction_for_get);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  delete transaction_for_get;

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  LOG_INFO("[concurrent-iterator] start.\n");
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);
  

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  //int64_t scale_factor = 10;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  Transaction *transaction_for_get = new Transaction(-1);
  std::vector<RID> rids;
  GenericKey<8> index_key;
  LOG_INFO("[concurrent-GetValue] start.\n");
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids, transaction_for_get);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  delete transaction_for_get;

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  LOG_INFO("[concurrent-iterator] start.\n");
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
  // create b+ tree
  //BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys;
  for (int i = 1; i <= 100; i++) keys.push_back(i);
  //std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {10, 1, 8, 9, 7, 2, 3, 6, 4, 5, 99, 97, 100, 98, 96};
  LaunchParallelTest(4, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 11;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  LOG_INFO("[concurrent-iterator] start.\n");
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key += 1;
    size += 1;
  }

  EXPECT_EQ(size, 85);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_MixTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 5);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  //std::vector<int64_t> remove_keys = {10, 1, 8, 9, 7, 2, 3, 6, 4, 5, 99, 97, 100, 98, 96};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    size += 1;
  }

  EXPECT_EQ(size, 5);
  //EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}


void InsertDeleteOne(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> &tree,
	bool* ret, uint64_t thread_itr) {
	ret[thread_itr] = true;
	GenericKey<8> index_key;
	RID rid;
	std::vector<RID> values;
	// create transaction
	Transaction *transaction = new Transaction(0);
  Transaction *transaction_for_read = new Transaction(-1);
	for (int i = 0; i < 2000; ++i) {
		int64_t value = (int64_t)thread_itr;
		rid.Set((int32_t)(value), value);
		index_key.SetFromInteger(value);
    
		if (!tree.Insert(index_key, rid, transaction))
		{
			ret[thread_itr] = false;
			break;
		}
		if (!tree.GetValue(index_key, &values,transaction_for_read))
		{
			ret[thread_itr] = false;
			break;
		}
		tree.Remove(index_key, transaction);
		if (tree.GetValue(index_key, &values,transaction_for_read))
		{
			ret[thread_itr] = false;
			break;
		}
	}
	delete transaction;
  delete transaction_for_read;
}

TEST(BPlusTreeConcurrentTest, DISABLED_RootLatchTest) {
	// create KeyComparator and index schema
	Schema *key_schema = ParseCreateStatement("a bigint");
	GenericComparator<8> comparator(key_schema);

	DiskManager *disk_manager = new DiskManager("test.db");
	BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);
	// create b+ tree
	BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
	//GenericKey<8> index_key;
	RID rid;

	// create and fetch header_page
	page_id_t page_id;
	auto header_page = bpm->NewPage(&page_id);
	(void)header_page;
	
	int times = 3;
	bool* ans = new bool[times];
	LaunchParallelTest(times, InsertDeleteOne, std::ref(tree), ans);

	for (int i = 0; i < times; ++i)
	{
		EXPECT_EQ(ans[i], true);
	}
	delete[] ans;

	EXPECT_EQ(tree.IsEmpty(), true);

	bpm->UnpinPage(HEADER_PAGE_ID, true);
	delete disk_manager;
	delete bpm;
	remove("test.db");
	remove("test.log");

  delete key_schema;
}

int64_t GetSetRandomValue(std::set<int64_t>& value_sets)
{
	assert(!value_sets.empty());
	int pos = rand() % value_sets.size();
	for (auto i = value_sets.begin(); i != value_sets.end(); ++i)
	{
		if (pos == 0) {
			return *i;
		}
		--pos;
	}
	assert(false);
}

void InsertDeleteRandom(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> &tree,
	std::set<int64_t>* value_sets, uint64_t thread_itr) {
  // get tid 
  std::thread::id thread_id = std::this_thread::get_id();
  uint32_t* tidp = reinterpret_cast<uint32_t*>(&thread_id);
  uint32_t tid = (*tidp)%10000;


	std::set<int64_t>& value_set = value_sets[thread_itr];
	std::set<int64_t> delete_set;
	GenericKey<8> index_key;
	RID rid;
	std::vector<RID> values;
	// create transaction
	Transaction *transaction = new Transaction(0);
	for (int i = 0; i < 1000; ++i) {
		bool isDeleted;
		int64_t value;
		if (value_set.empty()){
        isDeleted = false;
        if (delete_set.empty())	{
            value = (int64_t)(1000 * thread_itr + rand() % 200);
        }	else {
            value = GetSetRandomValue(delete_set);
        }
		}	else {
        isDeleted = rand() % 100 < 40;
        if (isDeleted) {
            value = GetSetRandomValue(value_set);
        }	else{
          if (delete_set.empty())	{
              value = (int64_t)(1000 * thread_itr + rand() % 200);
              if (value_set.find(value) != value_set.end())	{
                  continue;
              }
          }	else {
              value = GetSetRandomValue(delete_set);
          }
        }
		}
		rid.Set((int32_t)(value), value);
		index_key.SetFromInteger(value);
		if (isDeleted) {
        bool isSuccess = tree.Remove(index_key, transaction);
        value_set.erase(value);
        delete_set.insert(value);
        if (!isSuccess) {
           LOG_INFO("[%u-InsertDeleteRandomf] test delete %ld failed.", tid, value);
        } else {
            LOG_INFO("[%u-InsertDeleteRandom] test delete %ld success.", tid, value);
        }
		}	else {
        bool isSuccess = tree.Insert(index_key, rid, transaction);
        value_set.insert(value);
        delete_set.erase(value);
        if (!isSuccess) {
           LOG_INFO("[%u-InsertDeleteRandomf] test insert %ld failed.", tid, value);
        } else {
           LOG_INFO("[%u-InsertDeleteRandom] test insert %ld success.", tid, value);
        }
		}
	}
	delete transaction;
}

TEST(BPlusTreeConcurrentTest, RandomTest) {
	// create KeyComparator and index schema
	Schema *key_schema = ParseCreateStatement("a bigint");
	GenericComparator<8> comparator(key_schema);

	DiskManager *disk_manager = new DiskManager("test.db");
	BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
	// create b+ tree
	BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 15, 15);
	//GenericKey<8> index_key;
	RID rid;

	// create and fetch header_page
	page_id_t page_id;
	auto header_page = bpm->NewPage(&page_id);
	(void)header_page;

	int times = 4;
	std::set<int64_t>* value_sets = new std::set<int64_t>[times];
	LaunchParallelTest(times, InsertDeleteRandom, std::ref(tree), value_sets);
	std::set<int64_t> all_values;
	for (int i = 0; i < times; ++i){
		for (auto j = value_sets[i].begin(); j != value_sets[i].end(); ++j){
			all_values.insert(*j);
		}
	}
	delete[] value_sets;


  uint64_t size = 0;
  uint64_t valuesize = all_values.size();
  uint64_t pre;
  for (auto it = tree.begin(); it != tree.End(); ++it) {
    uint64_t key = (*it).first.ToString();
    if (size != 0) {
      LOG_INFO("[concurrentTest-random] <pre, current> = <%lu, %lu>", pre, key);
      EXPECT_EQ(pre < key, true);
    }
    pre = key;
    auto sit = all_values.find(key);
    EXPECT_EQ(sit != all_values.end(), true);
    if(sit != all_values.end()) all_values.erase(sit);
    size += 1;
  }
  EXPECT_EQ(size, valuesize); 

  if(!all_values.empty()){
      LOG_INFO("[concurrentTest-random] left value in value set:");
      for (auto it = all_values.begin(); it != all_values.end(); it++){
          uint64_t key = *it;
          LOG_INFO("%lu", key);
      }
  }



	//printf("value set size:%lu\n", all_values.size());
  /*
	uint64_t size = 0;
	for (auto iterator = tree.begin(); iterator != tree.End(); ++iterator) {
		int64_t key  = (*iterator).first.ToString();
		EXPECT_EQ(all_values.find(key) != all_values.end(), true);
		size = size + 1;
	}
	EXPECT_EQ(size, all_values.size()); */

	bpm->UnpinPage(HEADER_PAGE_ID, true);
	delete disk_manager;
	delete bpm;
	remove("test.db");
	remove("test.log");

  delete key_schema;
}


}  // namespace bustub
