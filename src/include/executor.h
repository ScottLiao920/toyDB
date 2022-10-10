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
  virtual void Next(void *dst) = 0;
  virtual void End() = 0;
};

class scanExecutor : protected executor {
  // abstract class for scan executors
 protected:
  rel *table_;
  comparison_expr* qual_;
  bufferPoolManager *bpmgr_;
 public:
  void Init() override = 0;
  void SetMode(execution_mode);
  void SetTable(rel *);
  void SetBufferPoolManager(bufferPoolManager *);
  std::string GetTable() { return this->table_->GetName(); }
  void SetQual(comparison_expr* qual) { this->qual_ = qual; }
  void Next(void *dst) override = 0;
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
  void Next(void *dst) override;
  void End() override;
};

class indexScanExecutor : scanExecutor {
  RelID idx_table_;
 public:
  void Init() override;
  void Next(void *dst) override;
  void End() override;
};

class bitMapIndexScanExecutor : scanExecutor {
  RelID idx_table_;
 public:
  void Init() override;
  void Next(void *dst) override;
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

class aggregateExecutor : executor {
  void Init() override;
  void Next(void *) override;
  void End() override;
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
  // This executor update toyDBTUPLE(s) in a table
};

class indexExecutor : executor {
  // This executor build an index on a column;
 private:
  index_type type_;
  bufferPoolManager *bpmgr_;
 public:
  void Init() override;
  void Init(bufferPoolManager *, rel *, size_t, index_type);
};

class sortExecutor : executor {
  // This executor sort a column inside a table;
};

class selectExecutor : executor {
  // this executor emits the result tuple from underlying executors to user
 private:
  std::vector<expr *> targetList_;
  std::vector<executor *> children_;
  size_t cnt_ = 0;
 public:
  void Init() override {};
  void addChild(executor *exec) { this->children_.push_back(exec); };
  void Init(std::vector<expr *>, std::vector<executor *>);
  void Next(void *) override;
  void End() override;
};

#endif //TOYDB_SRC_INCLUDE_EXECUTOR_H_