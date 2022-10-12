// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 9/23/2022.
//

#include "btree.h"

template<class T>
bTreeNode<T>::bTreeNode(int t, bool is_leaf) {
  this->isLeaf_ = is_leaf;
  this->t_ = t;
  this->keys_ = std::vector<T>(2 * t - 1);
  this->children_.reserve(2 * t);// = std::vector<bTreeNode<T> *>(2 * t - 1);
  this->loc_ = std::vector<tupleLocType>(2 * t - 1);
  this->cnt_ = 0;
}
template<class T>
void bTreeNode<T>::insertNonFull(T k, tupleLocType loc) {
  int i = this->cnt_ - 1;
  if (this->isLeaf_ == true) {
	while (i >= 0 && this->keys_[i] > k) {
	  this->keys_[i + 1] = this->keys_[i];
	  i--;
	}
	this->keys_[i + 1] = k;
	this->loc_[i + 1] = loc;
	this->cnt_ = this->cnt_ + 1;
  } else {
	while (i >= 0 && this->keys_[i] > k)
	  i--;
	if (this->children_[i + 1]->cnt_ == 2 * this->t_ - 1) {
	  this->splitChild(i + 1, this->children_[i + 1]);
	  if (this->keys_[i + 1] < k)
		i++;
	}
	this->children_[i + 1]->insertNonFull(k, loc);
  }
}
template<class T>
void bTreeNode<T>::splitChild(int i, bTreeNode *y) {
  auto *z = new bTreeNode<T>(y->t_, y->isLeaf_);
  z->cnt_ = this->t_ - 1;
  for (int j = 0; j < this->t_ - 1; j++)
	z->keys_[j] = y->keys_[j + this->t_];
  if (y->isLeaf_ == false) {
	for (int j = 0; j < this->t_; j++)
	  z->children_[j] = y->children_[j + this->t_];
  }
  y->cnt_ = this->t_ - 1;
  for (int j = this->cnt_; j >= i + 1; j--)
	this->children_[j + 1] = this->children_[j];
  this->children_[i + 1] = z;
  for (int j = this->cnt_ - 1; j >= i; j--)
	this->keys_[j + 1] = this->keys_[j];
  this->keys_[i] = y->keys_[this->t_ - 1];
  ++this->cnt_;
}
template<class T>
void bTreeNode<T>::traverse() {
  unsigned int i;
  for (i = 0; i < this->cnt_; ++i) {
	if (not this->isLeaf_) {
	  this->children_[i]->traverse();
	}
	std::cout << " " << this->keys_[i];
  }
  if (not this->isLeaf_) {
	this->children_[i]->traverse();
  }
}
template<class T>
bTreeNode<T> *bTreeNode<T>::search(int k) {
  int i = 0;
  while (i < this->cnt_ && k > this->keys_[i])
	i++;
  if (this->keys_[i] == k)
	return this;
  if (this->isLeaf_ == true)
	return nullptr;
  return this->children_[i]->search(k);
}
template<class T>
void bTree<T>::insert(T k, tupleLocType loc) {
  if (root_ == NULL) {
	root_ = new bTreeNode<T>(t_, true);
	root_->keys_[0] = k;
	root_->cnt_ = 1;
  } else {
	if (root_->cnt_ == 2 * t_ - 1) {
	  auto *s = new bTreeNode<T>(t_, false);
	  s->children_[0] = root_;
	  s->splitChild(0, root_);
	  int i = 0;
	  if (s->keys_[0] < k)
		i++;
	  s->children_[i]->insertNonFull(k, loc);
	  root_ = s;
	} else
	  root_->insertNonFull(k, loc);
  }
}

template
class bTreeNode<int>;

template
class bTree<int>;
