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
  p.parse("SELECT SUM(*) as fck, col1 from table1, table2 where table1.relId_ = table2.relId_ and tabel2.col2 < 100");

  rel table1, table2;
  table1.set_name_("A");
  table2.set_name_("B");
  table1.add_column("relId_", sizeof(int), typeid(int));
  table1.add_column("content", 4, typeid(std::string));
  table2.add_column("relId_", sizeof(int), typeid(int));
  table2.add_column("col2", sizeof(float), typeid(float));
  int id = 0;
  std::string tmp_string("A");
  for (unsigned int i = 0; i < 100; ++i) {
	char buf[sizeof(int) + 4];
	std::memset(buf, 0, 4 + 4);
	std::memcpy(buf, &id, sizeof(int));
	std::memcpy(buf+sizeof(int), tmp_string.c_str(), 4);
	table1.add_row(sizeof(int) + 4);
	table1.rows_.back().pages_.push_back(1);
	table1.update_row(&bpmgr, i, buf);
	++id;
  }
  comparison_expr qual;
  seqScanExecutor seq_scan_executor;
  seq_scan_executor.Init(&table1, &bpmgr, qual);
  for (unsigned int i = 0; i < 100; ++i) {
	std::vector<toyDBTUPLE> tup(BATCH_SIZE);
	seq_scan_executor.Next(&tup);
	size_t offset = 0;
	size_t col_id = 0;
	for (auto col_size : tup.cbegin()->sizes_) {
	  // TODO: validate targetList on tmp_buf here
	  char tmp_buf[col_size];
	  std::memcpy(tmp_buf, tup.cbegin()->content_ + offset, col_size);
	  switch (type_schema.typeID2type[table1.cols_[col_id].typeid_]) {
		case (1): {
		  std::cout << "|" << (int)*tmp_buf;
		  break;
		}
		case (2): {
		  std::cout << "|" << (float)*tmp_buf;
		  break;
		}
		case (3): {
		  std::cout << "|" << (size_t)*tmp_buf;
		  break;
		}
		case (4): {
		  std::cout << "|" << *tmp_buf;
		  break;
		}
	  }
	  offset += col_size;
	  ++col_id;
	}
	std::cout << std::endl;
  }
  seq_scan_executor.End();
//  testBTree();
  return 0;
}
