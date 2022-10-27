// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

#include <iostream>
#include "storage.h"
#include "bufferpool.h"
#include "parser.h"
#include "executor.h"
#include "btree.h"
#include "type.h"
#include "planner.h"

void testBTree() {
  bTree<int> t(3);
  for (auto i = 0; i < 10; ++i) {
	t.insert(i + 10, tupleLocType(0, i));
  }

  std::cout << "The B-tree is: ";
  t.traverse();
  int k = 10;
  (t.search(k) != nullptr) ? std::cout << std::endl
									   << k << " is found"
						   : std::cout << std::endl
									   << k << " is not Found";

  k = 2;
  (t.search(k) != nullptr) ? std::cout << std::endl
									   << k << " is found"
						   : std::cout << std::endl
									   << k << " is not Found\n";
}

int main() {
  storageManager stmgr;
  stmgr.addPage();
  stmgr.addPage();
  bufferPoolManager bpmgr(&stmgr);
//  const char *tmp = "hello world";
//  char *dst = (char *)malloc(8192);
//  stmgr.writePage(0, (void *)tmp);
////    for (auto i = 0; i < 12; i++) {
////        bpmgr.readFromDisk(0);
////    }
//  for (auto i = 0; i < 10; i++)
//	bpmgr.insertToFrame(0, tmp, 12);
//  stmgr.readPage(0, (void *)dst);
//  for (auto i = 0; i < BUFFER_POOL_SIZE; i++) {
//	bpmgr.printContent(i);
//  }
//  bpmgr.writeToDisk(0, (size_t)0);
//  std::cout << dst << std::endl;

  parser p;
  planner o;
  o.SetBufferPoolManager(&bpmgr);

  rel table1, table2;
  table1.SetName("table1");
  table2.SetName("table2");
  table1.add_column("relId_", sizeof(int), typeid(int));
  table1.add_column("content", 4, typeid(std::string));
  table2.add_column("relId_", sizeof(int), typeid(int));
  table2.add_column("content", 4, typeid(std::string));
  int id = 0;
  std::string tmp_string("A");
  for (unsigned int i = 0; i < 100; ++i) {
	char buf[sizeof(int) + 4];
	std::memset(buf, 0, 4 + 4);
	std::memcpy(buf, &id, sizeof(int));
	std::memcpy(buf + sizeof(int), tmp_string.c_str(), 4);
	table1.add_row(sizeof(int) + 4);
	table1.rows_.back().pages_.push_back(0); // store table1 in physical page id 0
	table1.update_row(&bpmgr, i, buf);
	++id;
  }
  id = 0;
  tmp_string.push_back('B');
  for (unsigned int i = 0; i < 100; ++i) {
	char buf[sizeof(int) + 4];
	std::memset(buf, 0, 4 + 4);
	std::memcpy(buf, &id, sizeof(int));
	std::memcpy(buf + sizeof(int), tmp_string.c_str(), 4);
	table2.add_row(sizeof(int) + 4);
	table2.rows_.back().pages_.push_back(1);
	table2.update_row(&bpmgr, i, buf); // store table1 in physical page id 1
	++id;
  }

  p.parse(
	  "SELECT table1.relId_, table1.content, table2.content from table1, table2 where table1.relId_ = table2.relId_ and table2.relId_ >= 90 and table1.relId_<95");
  o.plan(p.stmt_tree_);
  o.Init();
  o.execute();

  p.parse("CREATE TABLE tmp_table (col1 int, col2 string(10)");
  o.plan(p.stmt_tree_);
  o.Init();
  o.execute();

  p.parse("SELECT tmp_table.col1 from tmp_table");
  o.plan(p.stmt_tree_);
  o.Init();
  o.execute();
//  comparison_expr qual1;
//  qual1.comparision_type = comparision::ngt;
//  qual1.data_srcs.emplace_back(parser::processDataSrc("TABLE1.RELID_"));
//  qual1.data_srcs.emplace_back(parser::processDataSrc("50"));
//  comparison_expr qual2;
//  qual2.comparision_type = comparision::ngt;
//  qual2.data_srcs.emplace_back(parser::processDataSrc("TABLE2.RELID_"));
//  qual2.data_srcs.emplace_back(parser::processDataSrc("50"));
//  seqScanExecutor seq_scan_executor_tab1, seq_scan_executor_tab2;
//  seq_scan_executor_tab1.Init(&table1, &bpmgr, &qual1);
//  seq_scan_executor_tab2.Init(&table2, &bpmgr, &qual2);
//  nestedLoopJoinExecutor nested_loop_join_executor;
//  nested_loop_join_executor.SetLeft(&seq_scan_executor_tab1);
//  nested_loop_join_executor.SetRight(&seq_scan_executor_tab2);
//  nested_loop_join_executor.Init();
//  nested_loop_join_executor.SetBufferPoolManager(&bpmgr);
//  comparison_expr join_predicate;
//  join_predicate.comparision_type = comparision::equal;
//  join_predicate.data_srcs.emplace_back(parser::processDataSrc("TABLE1.RELID_"));
//  join_predicate.data_srcs.emplace_back(parser::processDataSrc("TABLE1.RELID_"));
//  nested_loop_join_executor.SetPredicate(&join_predicate);
//  for (unsigned int i = 0; i < 100; ++i) {
//	std::vector<toyDBTUPLE> tup(BATCH_SIZE);
//	nested_loop_join_executor.Next(&tup);
//	if (tup[0].content_ == nullptr) {
//	  continue;
//	}
//	size_t offset = 0;
//	size_t col_id = 0;
//	for (auto col_size : tup.cbegin()->sizes_) {
//	  if (col_size == 0) {
//		continue;
//	  }
//	  char tmp_buf[col_size];
//	  std::memcpy(tmp_buf, tup.cbegin()->content_ + offset, col_size);
//	  switch (type_schema.typeID2type[tup.begin()->type_ids_[col_id]]) {
//		case (1): {
//		  std::cout << "|" << (int)*tmp_buf;
//		  break;
//		}
//		case (2): {
//		  std::cout << "|" << (float)*tmp_buf;
//		  break;
//		}
//		case (3): {
//		  std::cout << "|" << (size_t)*tmp_buf;
//		  break;
//		}
//		case (4): {
//		  std::cout << "|" << std::string(tmp_buf);
//		  break;
//		}
//	  }
//	  offset += col_size;
//	  ++col_id;
//	}
//	std::cout << std::endl;
//  }
//  nested_loop_join_executor.End();
////  testBTree();
  return 0;
}
