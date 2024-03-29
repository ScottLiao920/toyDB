// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 9/22/2022.
//

#ifndef TOYDB_SRC_INCLUDE_EXECUTOR_H_
#define TOYDB_SRC_INCLUDE_EXECUTOR_H_

#include "common.h"
#include "bufferpool.h"
#include "parser.h"
#include "btree.h"

class executor {
  //abstract class for all the executors
 protected:
  std::unordered_map<char *, size_t> free_spaces_;
  std::unordered_map<char *, size_t> allocated_spaces;
  char mem_context_[EXEC_MEM];
  char *ptr = mem_context_;
  execution_mode mode_ = volcano;
  bufferPoolManager *bpmgr_;
  rel *view_; // TODO: update view's col & row
//  std::vector<RelID> views_;
//  std::vector<PhysicalPageID> on_disk_views_;
//  inMemoryView *view_; // save to disk if buffer pool is full
 public:
  virtual void Init();
  virtual void Next(void *dst) = 0;
  virtual void End();
  char *talloc(size_t);
  void tfree(char *, size_t);
  void tfree(char *);
  void SetBufferPoolManager(bufferPoolManager *);
  std::string GetViewName() { return this->view_->GetName(); };
  RelID GetViewID() { return this->view_->GetID(); };
};

class scanExecutor : public executor {
  // abstract class for scan executors
 protected:
  rel *table_;
  std::vector<comparison_expr *> quals_; // Should consider multiple predicate on a single executor
 public:
  void Init() override = 0;
  void SetMode(execution_mode);
  void SetTable(rel *);
  std::string GetTableName() { return this->table_->GetName(); }
  size_t GetTableID() { return this->table_->GetID(); }
  void AddQual(comparison_expr *qual) { this->quals_.emplace_back(qual); }
  void Next(void *dst) override = 0;
  void End() override = 0;
};

class seqScanExecutor : public scanExecutor {
 private:
  unsigned int cnt_;
  char *mem_ptr_ = nullptr;
  std::vector<PhysicalPageID> pages_;
 public:
  void Init() override;
  void Init(rel *, bufferPoolManager *, comparison_expr *);
  void Next(void *dst) override;
  void Reset();
  void End() override;
};

class indexScanExecutor : public scanExecutor {
  RelID idx_table_;
 public:
  void Init() override {};
  void Next(void *dst) override {};
  void End() override {};
};

class bitMapIndexScanExecutor : public scanExecutor {
  RelID idx_table_;
 public:
  void Init() override {};
  void Next(void *dst) override {};
  void End() override {};
};

class joinExecutor : public executor {
 protected:
  // abstract class for all join executors
  comparison_expr *pred_ = nullptr;
  executor *left_child_ = nullptr;
  executor *right_child_ = nullptr;
  size_t offset_ = 0;
 public:
  void Init() override = 0;
  void Next(void *dst) override = 0;
  void End() override = 0;
  toyDBTUPLE *Join(toyDBTUPLE *, toyDBTUPLE *);
};

class nestedLoopJoinExecutor : public joinExecutor {
 private:
  std::vector<toyDBTUPLE> curLeftBatch_;
  std::vector<toyDBTUPLE>::iterator curLeftTuple_;
  std::vector<toyDBTUPLE> curRightBatch_;
  std::vector<toyDBTUPLE>::iterator curRightTuple_;
 public:
  void Init() override;
  void SetLeft(executor *left) { this->left_child_ = left; };
  void SetRight(executor *right) { this->right_child_ = right; };
  void SetPredicate(comparison_expr *tmp) { this->pred_ = tmp; };
  void Next(void *dst) override;
  void End() override;
};

class hashJoinExecutor : public joinExecutor {
 private:
  void *left_index_tree_ = nullptr;
  std::string col_name;
  toyDBTUPLE curLeftTuple;
 public:
  void Init() override;
  void SetLeft(executor *left) { this->left_child_ = left; };
  void SetRight(executor *right) { this->right_child_ = right; };
  void SetPredicate(comparison_expr *tmp) { this->pred_ = tmp; };
  void Next(void *dst) override;
  void End() override;
};

class mergeJoinExecutor : joinExecutor {
  void Init() override {};
  void Next(void *dst) override {};
  void End() override {};
};

class aggregateExecutor : public executor {
 protected:
  executor *child_ = nullptr;
  size_t col_idx;
  size_t col_offset;
  unsigned char result_[64];
  std::string column_name_;
  bool finished_ = false;
 public:
  void Init() override;
  void SetChild(executor *inp) { this->child_ = inp; }
  void SetColumn(std::string);
  void Next(void *) override = 0;
  void End() override;
};

class SumAggregateExecutor : public aggregateExecutor {
 public:
  void Init() override;
  void Next(void *) override;
};

class CountAggregateExecutor : public aggregateExecutor {
 public:
  void Init() override;
  void Next(void *) override;
//  void End() override;
};

class AvgAggregateExecutor : public aggregateExecutor {
 public:
  void Init() override;
  void Next(void *) override;
};

class MaxAggregateExecutor : public aggregateExecutor {
 public:
  void Init() override;
  void Next(void *) override;
};

class MinAggregateExecutor : public aggregateExecutor {
 public:
  void Init() override;
  void Next(void *) override;
};

class createExecutor : executor {
  // This executor creates a table
 private:
  storageMethod default_method = row_store;
  std::string name_;
  std::vector<std::tuple<size_t, size_t, size_t, std::string>> cols_;
 public:
  void Init() override;
  void SetName(std::string &inp) { name_ = std::move(inp); }
  void SetCols(std::vector<std::tuple<size_t, size_t, size_t, std::string>> &inp) { cols_ = std::move(inp); }
  void Next(void *dst) override { std::memset(dst, 0, 8); };
};

class dropExecutor : executor {
  // This executor drops a table from schema;
};

class insertExecutor : executor {
  // This executor insert a row into a table
};

class copyExecutor : executor {
  // This executor insert bunch of rows into a table
 private:
  storageMethod default_method = row_store;
  std::string name_;

  bool is_from_ = false;
  std::string file_path;
 public:
  void SetName(std::string &inp) { name_ = std::move(inp); }
  void SetPath(std::string inp) { this->file_path = std::move(inp); }
  void SetDirection(bool inp) { this->is_from_ = inp; }
  void Init() override;
  void Next(void *dst) override { std::memset(dst, 0, 8); };
};

class updateExecutor : executor {
  // This executor update toyDBTUPLE(s) in a table
  rel *table_ = nullptr;
  size_t cnt_ = 0;
 public:
  void Init() override;
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
  void Init() override;
  void addChild(executor *exec) { this->children_.push_back(exec); };
  void addTarget(expr *target) { this->targetList_.push_back(target); };
  void Init(std::vector<expr *>, std::vector<executor *>);
  void Next(void *) override;
  void End() override;
};

#endif //TOYDB_SRC_INCLUDE_EXECUTOR_H_