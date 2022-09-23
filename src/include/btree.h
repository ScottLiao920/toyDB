//
// Created by scorp on 9/23/2022.
//

#ifndef TOYDB_SRC_INCLUDE_BTREE_H_
#define TOYDB_SRC_INCLUDE_BTREE_H_

#include "common.h"

typedef std::tuple<PhysicalPageID, size_t> tupleLocType;

template<class T>
class bTreeNode {
 private:
  size_t t_{};
  size_t cnt_{};
  std::vector<T> keys_;
  bool isLeaf_{};
  std::vector<bTreeNode<T> *> children_;
  std::vector<tupleLocType> loc_;
 public:
  explicit bTreeNode<T>(int t, bool is_leaf);
  void insertNonFull(T k, tupleLocType loc);
  void splitChild(int i, bTreeNode<T> *y);
  void traverse();
  bTreeNode<T> *search(int k);
  friend class BTree;
};

template<class T>
class bTree {
 private:
  bTreeNode<T> *root_;
  size_t t_;
 public:
  explicit bTree<T>(size_t temp) : root_(nullptr), t_(temp) {};
  void traverse() {
	if (this->root_ != NULL)
	  root_->traverse();
  }
  bTreeNode<T> *search(T k) {
	return (this->root_ == NULL) ? NULL : this->root_->search(k);
  }
  void insert(T k, tupleLocType loc);
};

#endif //TOYDB_SRC_INCLUDE_BTREE_H_
