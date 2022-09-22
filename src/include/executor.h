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
  char memory_context_[EXEC_MEM] = "";
 public:
  virtual void Init();
  virtual std::vector<tuple> Next();
  virtual void End();
};

class createExecutor : executor {
  // This executor creates a table
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
};

class sortExecutor : executor {
  // This executor sort a column inside a table;
};

class scanExecutor : executor {
  // abstract class for scan executors
  RelID table_;
  comparison_expr qual_;
  storageManager *stmgr_;
 public:
  void Init() override = 0;
  void SetMode(execution_mode);
  void SetTable(RelID);
  void SetStorageManager(storageManager *);
  std::vector<tuple> Next() override = 0;
  void End() override = 0;
};

class seqScanExecutor : scanExecutor {
 public:
  void Init() override;
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

#endif //TOYDB_SRC_INCLUDE_EXECUTOR_H_
