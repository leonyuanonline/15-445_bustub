#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  LOG_DEBUG("called");
  // Initialize the child executor
  topn_tuples_.clear();
  child_executor_->Init();

  // Comparator for comparing two pairs of (Tuple, RID), only compares the Tuple part
  auto tuple_comparator = [&](const TupleWithRID &lhs, const TupleWithRID &rhs) -> bool {
    const auto &order_bys = plan_->GetOrderBy();
    for (const auto &[order_type, expr] : order_bys) {
      Value lhs_val = expr->Evaluate(&lhs.first, child_executor_->GetOutputSchema());
      Value rhs_val = expr->Evaluate(&rhs.first, child_executor_->GetOutputSchema());

      OrderByType order = order_type == OrderByType::DEFAULT ? OrderByType::ASC : order_type;
      if (lhs_val.CompareLessThan(rhs_val) == CmpBool::CmpTrue) {
        return order == OrderByType::ASC;  // ASC: lhs < rhs
      }
      if (lhs_val.CompareGreaterThan(rhs_val) == CmpBool::CmpTrue) {
        return order == OrderByType::DESC;  // DESC: lhs > rhs
      }
    }
    return false;
  };

  // Create a min-heap (priority queue) to store the top N tuples
  std::priority_queue<TupleWithRID, std::vector<TupleWithRID>, decltype(tuple_comparator)> min_heap(tuple_comparator);

  Tuple tuple;
  RID rid;
  size_t limit_n = plan_->GetN();

  // Fetch tuples from the child executor and keep only the top N tuples
  while (child_executor_->Next(&tuple, &rid)) {
    min_heap.push({tuple, rid});
    if (min_heap.size() > limit_n) {
      min_heap.pop();  // Remove the smallest element if size exceeds N
    }
  }

  // Transfer the top N tuples from the heap to a sorted vector (reverse order)
  while (!min_heap.empty()) {
    topn_tuples_.push_back(min_heap.top());
    min_heap.pop();
  }

  // Reverse the vector to get tuples in the correct order
  std::reverse(topn_tuples_.begin(), topn_tuples_.end());

  // Initialize the iterator to return tuples
  topn_tuples_iter_ = topn_tuples_.begin();

  //   display sorted tuples
  for (auto &tuple : topn_tuples_) {
    LOG_DEBUG("sorted tuple: %s", tuple.first.ToString(&plan_->OutputSchema()).c_str());
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Check if there are tuples left to return
  if (topn_tuples_iter_ == topn_tuples_.end()) {
    return false;
  }

  // Return the next tuple
  *tuple = topn_tuples_iter_->first;
  *rid = topn_tuples_iter_->second;
  ++topn_tuples_iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  return topn_tuples_.size();  // For testing purposes
}

}  // namespace bustub
