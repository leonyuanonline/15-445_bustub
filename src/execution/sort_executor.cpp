#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // Initialize the child executor to fetch all tuples.
  LOG_DEBUG("called");
  sorted_tuples_.clear();
  child_executor_->Init();
  Tuple tuple;
  RID rid;

  // Fetch all tuples from the child executor and store them in a vector.
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.emplace_back(tuple);
  }

  // Sort the tuples based on the order_bys from the plan.
  const auto &order_bys = plan_->GetOrderBy();
  auto comparator = [&](const Tuple &a, const Tuple &b) -> bool {
    for (const auto &[order_type, expr] : order_bys) {
      Value lhs = expr->Evaluate(&a, plan_->OutputSchema());
      Value rhs = expr->Evaluate(&b, plan_->OutputSchema());

      // 如果排序类型没有明确指定，默认使用 ASC
      OrderByType order = order_type == OrderByType::DEFAULT ? OrderByType::ASC : order_type;

      if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
        return order == OrderByType::ASC;  // 对于升序，lhs < rhs 时返回 true
      }
      if (lhs.CompareGreaterThan(rhs) == CmpBool::CmpTrue) {
        return order == OrderByType::DESC;  // 对于降序，lhs > rhs 时返回 true
      }
    }
    return false;  // 如果完全相等，不需要交换顺序
  };

  // Use std::sort to sort the tuples.
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), comparator);

  //   display sorted tuples
  //   for (auto &tuple : sorted_tuples_) {
  //     LOG_DEBUG("sorted tuple: %s", tuple.ToString(&plan_->OutputSchema()).c_str());
  //   }

  // Initialize the iterator.
  sorted_tuples_iter_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("called");
  // Check if there are still tuples left to return.
  if (sorted_tuples_iter_ == sorted_tuples_.end()) {
    return false;
  }

  // Return the next tuple.
  *tuple = *sorted_tuples_iter_;
  *rid = tuple->GetRid();
  ++sorted_tuples_iter_;
  return true;
}

}  // namespace bustub
