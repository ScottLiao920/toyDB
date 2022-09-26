#include <iostream>
#include "storage.h"
#include "bufferpool.h"
#include "parser.h"
#include "executor.h"
#include "btree.h"

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
  const char *tmp = "hello world";
  char *dst = (char *)malloc(8192);
  stmgr.writePage(0, (void *)tmp);
//    for (auto i = 0; i < 12; i++) {
//        bpmgr.readFromDisk(0);
//    }
  for (auto i = 0; i < 10; i++)
	bpmgr.insertToFrame(0, tmp, 12);
  stmgr.readPage(0, (void *)dst);
  for (auto i = 0; i < BUFFER_POOL_SIZE; i++) {
	bpmgr.printContent(i);
  }
  bpmgr.writeToDisk(0, (size_t)0);

  std::cout << dst;
  parser p;
  p.parse("SELECT SUM(*) as fck, col1 from table1, table2 where table1.relId_ = table2.relId_ and tabel2.col2 < 100");

  rel table1, table2;
  table1.set_name_("A");
  table2.set_name_("B");
  table1.add_column("relId_", 8);
  table2.add_column("relId_", 8);
  table2.add_column("col2", sizeof(float));
  char id[8] = "AAAAAAA";
//  id[0] = '1';
  for (unsigned int i = 0; i < 100; ++i) {
	table1.add_row(8);
	table1.rows_.back().pages_.push_back(1);
	table1.update_row(&bpmgr, i, id);
  }
  comparison_expr qual;
  seqScanExecutor seq_scan_executor;
  seq_scan_executor.Init(&table1, &bpmgr, qual);
  for (unsigned int i = 0; i < 100; ++i) {
	std::vector<toyDBTUPLE> tup;
	tup.reserve(BATCH_SIZE);
	seq_scan_executor.Next(&tup);
	std::cout << tup[0].content_ << std::endl;
  }
  seq_scan_executor.End();
  testBTree();
  return 0;
}
