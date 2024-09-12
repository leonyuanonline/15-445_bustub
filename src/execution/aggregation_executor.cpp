//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();  // 初始化子执行器

  LOG_DEBUG("AggregationExecutor::Init() called, initializing child executor and aggregation hash table");

  // 遍历子执行器的每个元组并进行聚合
  Tuple tuple;
  RID rid;
  size_t tuple_count = 0;
  aht_.Clear();
  while (child_executor_->Next(&tuple, &rid)) {
    AggregateKey agg_key = MakeAggregateKey(&tuple);
    AggregateValue agg_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(agg_key, agg_value);  // 插入并聚合值
    tuple_count++;

    // 打印插入的聚合键和值
  }

  LOG_DEBUG("AggregationExecutor::Init() completed, processed %lu tuples", tuple_count);

  // 初始化哈希表迭代器
  aht_iterator_ = aht_.Begin();
  empty_table_ = tuple_count == 0;
  LOG_DEBUG("AggregationExecutor::Init() initialized hash table iterator");
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 检查聚合表是否为空
  if (aht_iterator_ == aht_.End() && empty_table_) {
    LOG_DEBUG("AggregationExecutor::Next() called but aggregation hash table is empty");

    // 检查是否有GROUP BY子句
    const auto &group_bys = plan_->GetGroupBys();
    if (group_bys.empty()) {
      // 没有 GROUP BY 子句，返回全局聚合结果
      auto agg_values = aht_.GenerateInitialAggregateValue().aggregates_;

      // 使用生成的初始聚合值创建一个 Tuple
      *tuple = Tuple{agg_values, &GetOutputSchema()};
      empty_table_ = false;  // 防止死循环

      LOG_DEBUG("Returning initial aggregate value for empty table: %s", tuple->ToString(&GetOutputSchema()).c_str());
      return true;
    }
    // 有 GROUP BY 子句，空表直接返回 false，表示没有结果
    return false;
  }

  // 如果哈希表中有数据，继续处理
  if (aht_iterator_ != aht_.End()) {
    auto key = aht_iterator_.Key();
    auto val = aht_iterator_.Val();
    ++aht_iterator_;

    std::vector<Value> values;
    values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
    values.insert(values.end(), val.aggregates_.begin(), val.aggregates_.end());

    *tuple = Tuple(values, &GetOutputSchema());
    LOG_DEBUG("Returning aggregated tuple: %s", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
