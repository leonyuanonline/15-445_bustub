#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  all_tuples_.clear();
  window_results_.clear();
  current_tuple_idx_ = 0;

  // 从子执行器获取所有元组
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    all_tuples_.push_back(tuple);
  }
  for (const auto &tuple : all_tuples_) {
    LOG_DEBUG("tuple: %s", tuple.ToString(&child_executor_->GetOutputSchema()).c_str());
  }

  // 计算每个窗口函数
  for (const auto &[window_idx, window_func] : plan_->window_functions_) {
    LOG_DEBUG("window_idx: %d", window_idx);
    ComputeWindowFunction(window_idx, window_func);
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_tuple_idx_ >= all_tuples_.size()) {
    return false;
  }

  const auto &current_tuple = all_tuples_[current_tuple_idx_];
  std::vector<Value> values;

  // 添加窗口函数结果
  for (size_t i = 0; i < GetOutputSchema().GetColumnCount(); i++) {
    if (plan_->window_functions_.find(i) != plan_->window_functions_.end()) {
      values.push_back(window_results_[i][current_tuple_idx_]);
    } else {
      // 对于非窗口函数列，从原始 tuple 中获取值
      values.push_back(current_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
    }
  }

  *tuple = Tuple(values, &GetOutputSchema());
  *rid = current_tuple.GetRid();
  current_tuple_idx_++;

  return true;
}

void WindowFunctionExecutor::ComputeWindowFunction(uint32_t window_idx,
                                                   const WindowFunctionPlanNode::WindowFunction &window_func) {
  SortAndPartitionTuples(window_func.partition_by_, window_func.order_by_);

  if (window_func.type_ == WindowFunctionType::Rank) {
    ComputeRank(window_idx, window_func);
  } else {
    std::vector<Value> result_values(all_tuples_.size());
    size_t partition_start = 0;
    std::vector<Value> partition_values;

    for (size_t i = 0; i <= all_tuples_.size(); ++i) {
      bool is_new_partition = i == all_tuples_.size() ||
                              (i > 0 && !SamePartition(all_tuples_[i - 1], all_tuples_[i], window_func.partition_by_));

      if (is_new_partition) {
        // 计算当前分区的结果
        Value aggregate_result = ComputeAggregate(window_func.type_, partition_values);

        // 如果没有 ORDER BY，则整个分区使用相同的结果
        if (window_func.order_by_.empty()) {
          for (size_t j = partition_start; j < i; ++j) {
            result_values[j] = aggregate_result;
          }
        }

        // 重置分区值
        partition_values.clear();
        partition_start = i;
      }

      if (i < all_tuples_.size()) {
        Value current_value = window_func.function_->Evaluate(&all_tuples_[i], child_executor_->GetOutputSchema());
        partition_values.push_back(current_value);

        // 如果有 ORDER BY，则计算累积结果
        if (!window_func.order_by_.empty()) {
          result_values[i] = ComputeAggregate(window_func.type_, partition_values);
        }
      }
    }
    LOG_DEBUG("window_idx: %d", window_idx);
    for (const auto &value : result_values) {
      LOG_DEBUG("value: %s", value.ToString().c_str());
    }
    window_results_[window_idx] = std::move(result_values);
  }
}

auto WindowFunctionExecutor::SamePartition(const Tuple &a, const Tuple &b,
                                           const std::vector<AbstractExpressionRef> &partition_by) -> bool {
  if (partition_by.empty()) {
    return true;  // 如果没有 PARTITION BY，所有元组都在同一个分区
  }
  for (const auto &expr : partition_by) {
    Value va = expr->Evaluate(&a, child_executor_->GetOutputSchema());
    Value vb = expr->Evaluate(&b, child_executor_->GetOutputSchema());
    if (va.CompareNotEquals(vb) == CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

void WindowFunctionExecutor::SortAndPartitionTuples(
    const std::vector<AbstractExpressionRef> &partition_by,
    const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by) {
  if (partition_by.empty() && order_by.empty()) {
    return;  // 如果既没有 PARTITION BY 也没有 ORDER BY，保持原始顺序
  }
  std::sort(all_tuples_.begin(), all_tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    // 首先按照 PARTITION BY 进行比较
    for (const auto &expr : partition_by) {
      Value va = expr->Evaluate(&a, child_executor_->GetOutputSchema());
      Value vb = expr->Evaluate(&b, child_executor_->GetOutputSchema());
      if (va.CompareNotEquals(vb) == CmpBool::CmpTrue) {
        return va.CompareLessThan(vb) == CmpBool::CmpTrue;
      }
    }

    // 然后按照 ORDER BY 进行比较
    for (const auto &[order_type, expr] : order_by) {
      Value va = expr->Evaluate(&a, child_executor_->GetOutputSchema());
      Value vb = expr->Evaluate(&b, child_executor_->GetOutputSchema());
      if (va.CompareNotEquals(vb) == CmpBool::CmpTrue) {
        return (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT)
                   ? (va.CompareLessThan(vb) == CmpBool::CmpTrue)
                   : (va.CompareGreaterThan(vb) == CmpBool::CmpTrue);
      }
    }

    return false;
  });
}

auto WindowFunctionExecutor::ComputeAggregate(WindowFunctionType type, const std::vector<Value> &values) -> Value {
  switch (type) {
    case WindowFunctionType::CountStarAggregate:
      return ValueFactory::GetIntegerValue(static_cast<int32_t>(values.size()));
    case WindowFunctionType::CountAggregate: {
      int32_t count = 0;
      for (const auto &value : values) {
        if (!value.IsNull()) {
          count++;
        }
      }
      return ValueFactory::GetIntegerValue(count);
    }
    case WindowFunctionType::SumAggregate: {
      Value sum = ValueFactory::GetIntegerValue(0);
      for (const auto &value : values) {
        if (!value.IsNull()) {
          sum = sum.Add(value);
        }
      }
      return sum;
    }
    case WindowFunctionType::MinAggregate: {
      if (values.empty()) {
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
      }
      Value min = values[0];
      for (const auto &value : values) {
        if (!value.IsNull() && (min.IsNull() || value.CompareLessThan(min) == CmpBool::CmpTrue)) {
          min = value;
        }
      }
      return min;
    }
    case WindowFunctionType::MaxAggregate: {
      if (values.empty()) {
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
      }
      Value max = values[0];
      for (const auto &value : values) {
        if (!value.IsNull() && (max.IsNull() || value.CompareGreaterThan(max) == CmpBool::CmpTrue)) {
          max = value;
        }
      }
      return max;
    }
    default:
      throw Exception("Unsupported window function type");
  }
}

void WindowFunctionExecutor::ComputeRank(uint32_t window_idx,
                                         const WindowFunctionPlanNode::WindowFunction &window_func) {
  std::vector<Value> result_values(all_tuples_.size());
  int32_t current_rank = 1;
  int32_t equal_rank_count = 0;

  for (size_t i = 0; i < all_tuples_.size(); ++i) {
    if (i > 0) {
      bool is_equal = true;
      for (const auto &[order_type, expr] : window_func.order_by_) {
        Value prev = expr->Evaluate(&all_tuples_[i - 1], child_executor_->GetOutputSchema());
        Value curr = expr->Evaluate(&all_tuples_[i], child_executor_->GetOutputSchema());
        if (prev.CompareNotEquals(curr) == CmpBool::CmpTrue) {
          is_equal = false;
          break;
        }
      }

      if (!is_equal) {
        current_rank += equal_rank_count + 1;
        equal_rank_count = 0;
      } else {
        equal_rank_count++;
      }
    }

    result_values[i] = ValueFactory::GetIntegerValue(current_rank);
  }

  window_results_[window_idx] = std::move(result_values);
}
}  // namespace bustub
