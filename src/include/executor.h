//
// Created by scorp on 9/22/2022.
//

#ifndef TOYDB_SRC_INCLUDE_EXECUTOR_H_
#define TOYDB_SRC_INCLUDE_EXECUTOR_H_

#include "common.h"
#include "bufferpool.h"
#include "parser.h"

class executor {
  //abstract class for all the executors
 protected:
  execution_mode mode_ = volcano;
 public:
  virtual void Init() = 0;
  virtual std::vector<tuple> Next() = 0;
  virtual void End() = 0;
};

class scanExecutor : protected executor {
  // abstract class for scan executors
 protected:
  rel *table_;
  comparison_expr qual_;
  bufferPoolManager *bpmgr_;
 public:
  void Init() override = 0;
  void SetMode(execution_mode);
  void SetTable(rel *);
  void SetBufferPoolManager(bufferPoolManager *);
  std::vector<tuple> Next() override = 0;
  void End() override = 0;
};

class seqScanExecutor : protected scanExecutor {
 private:
  unsigned int cnt_;
  char *mem_ptr_ = nullptr;
  std::vector<PhysicalPageID> pages_;
 public:
  void Init() override {};
  void Init(rel *, bufferPoolManager *, comparison_expr);
  std::vector<tuple> Next() override;
  void End() override;
};

class indexScanExecutor : scanExecutor {
  RelID idx_table_;
 public:
  void Init() override;
  std::vector<tuple> Next() override;
  void End() override;
};

class bitMapIndexScanExecutor : scanExecutor {
  RelID idx_table_;
 public:
  void Init() override;
  std::vector<tuple> Next() override;
  void End() override;
};

class joinExecutor : executor {
  // abstract class for all join executors
  comparison_expr pred_;
  executor *left_child_;
  executor *right_child_;
 public:
  void Init() override = 0;

};

class nestedLoopJoinExecutor : joinExecutor {

};

class hashJoinExecutor : joinExecutor {

};

class mergeJoinExecutor : joinExecutor {

};

class createExecutor : executor {
  // This executor creates a table
 private:
  storageManager *storage_manager_;

 public:
  void Init() override;
  void setStorageManager(storageManager *);

};

class dropExecutor : executor {
  // This executor drops a table from schema;
};

class insertExecutor : executor {
  // This executor insert a row/col into a table
};

class updateExecutor : executor {
  // This executor update tuple(s) in a table
};

class indexExecutor : executor {
  // This executor build an index on a column;
 private:
  index_type type_;
  storageManager *stmgr_;
 public:
  void Init() override;
  void Init(storageManager *, rel *, size_t, index_type);
};

class sortExecutor : executor {
  // This executor sort a column inside a table;
};

#endif //TOYDB_SRC_INCLUDE_EXECUTOR_H_