//
// Created by scorp on 9/22/2022.
//
#include "executor.h"

void executor::Init() {
  std::memset(this->memory_context_, 0, EXEC_MEM);
  this->mode_ = volcano;
}

void scanExecutor::SetMode(execution_mode mode) {
  this->mode_ = mode;
}
void scanExecutor::SetTable(rel *tab) {
  this->table_ = tab;
}
void scanExecutor::SetBufferPoolManager(bufferPoolManager *manager) {
  this->bpmgr_ = manager;
}
void joinExecutor::Init() {
  executor::Init();
  this->left_child_ = nullptr; // wait for planner to set it
  this->right_child_ = nullptr; // wait for planner to set it
}
void createExecutor::Init() {
  executor::Init();
}
void createExecutor::setStorageManager(storageManager *manager) {
  this->storage_manager_ = manager;
}

void seqScanExecutor::Init(rel *tab, bufferPoolManager *manager, comparison_expr qual) {
  this->table_ = tab;
  this->bpmgr_ = manager;
  this->qual_ = qual;
  this->cnt_ = 0;
  this->pages_ = this->table_->get_location();
  manager->stmgr_->readPage(this->pages_[0], this->memory_context_);
}
std::vector<tuple> seqScanExecutor::Next() {
  std::vector<tuple> out;
  if (this->mode_ == volcano) {
	// emit one at a time
	out.reserve(1);
	std::memcpy(out[0].content_, this->mem_ptr_, this->table_->get_tuple_size());
	this->mem_ptr_ += this->table_->get_tuple_size();
  } else {
	// emit a batch at a time
	out.reserve(BATCH_SIZE);
	for (auto &it : out) {
	  std::memcpy(it.content_, this->mem_ptr_, this->table_->get_tuple_size());
	  this->mem_ptr_ += this->table_->get_tuple_size();
	}
  }
  return out;
}
void seqScanExecutor::End() {
  free(this->memory_context_);
}
