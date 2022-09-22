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
void scanExecutor::SetTable(RelID tab) {
  this->table_ = tab;
}
void scanExecutor::SetStorageManager(storageManager *manager) {
  this->stmgr_ = manager;
}
void joinExecutor::Init() {
  executor::Init();
  this->left_child_ = nullptr; // wait for planner to set it
  this->right_child_ = nullptr; // wait for planner to set it
}
