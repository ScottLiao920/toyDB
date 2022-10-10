//
// Created by liaoc on 9/19/22.
//

#include "planner.h"

void planner::plan(queryTree *query_tree) {

}
void planner::execute() {
  for (;;) {
	this->cheapest_tree_->root->Next(nullptr);
  }
}
