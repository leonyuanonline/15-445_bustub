#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 检查当前的 plan 是否为 SeqScanPlanNode
  if (plan->GetType() != PlanType::SeqScan) {
    return plan;
  }

  // 使用 std::dynamic_pointer_cast 将 plan 转换为 const std::shared_ptr<const SeqScanPlanNode>
  auto seq_scan_plan = std::dynamic_pointer_cast<const SeqScanPlanNode>(plan);
  if (seq_scan_plan == nullptr) {
    return plan;
  }

  // 获取表的 OID 和表名
  table_oid_t table_oid = seq_scan_plan->GetTableOid();
  auto table_info = catalog_.GetTable(table_oid);
  if (table_info == nullptr) {
    return plan;
  }

  const auto &filter_predicate = seq_scan_plan->filter_predicate_;

  // 如果没有过滤条件，直接返回 SeqScan 计划
  if (filter_predicate == nullptr) {
    return plan;
  }

  // 检查是否有适用于过滤条件的索引
  auto indexes = catalog_.GetTableIndexes(table_info->name_);
  for (const auto &index_info : indexes) {
    const auto &index_key_attrs = index_info->index_->GetKeyAttrs();

    // 检查谓词是否为简单的等值比较
    if (auto comparison_expr = std::dynamic_pointer_cast<const ComparisonExpression>(filter_predicate)) {
      if (comparison_expr->comp_type_ == ComparisonType::Equal) {
        const auto &left_expr = comparison_expr->GetChildAt(0);
        const auto &right_expr = comparison_expr->GetChildAt(1);

        // 检查左表达式是否为 ColumnValueExpression
        if (auto column_ref_expr = std::dynamic_pointer_cast<const ColumnValueExpression>(left_expr)) {
          // 获取列索引并检查它是否与索引的第一列匹配
          auto col_idx = column_ref_expr->GetColIdx();
          if (col_idx == index_key_attrs[0]) {
            // 使用符合构造函数要求的参数创建 IndexScanPlanNode
            auto index_scan_plan = std::make_shared<IndexScanPlanNode>(
                std::make_shared<const Schema>(seq_scan_plan->OutputSchema()),  // 输出模式
                table_oid,                                                      // 表 OID
                index_info->index_oid_,                                         // 索引 OID
                filter_predicate,                                               // 过滤谓词
                dynamic_cast<ConstantValueExpression *>(right_expr.get()));     // 常量键

            return index_scan_plan;
          }
        }
      }
    }
  }

  // 如果没有适用的索引，则返回原始 plan
  return plan;
}

}  // namespace bustub
