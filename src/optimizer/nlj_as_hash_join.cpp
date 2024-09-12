#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
// 递归提取 AND 表达式中的等值条件
inline void ExtractJoinKeys(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &left_keys,
                            std::vector<AbstractExpressionRef> &right_keys) {
  if (predicate == nullptr) {
    return;
  }

  // 动态转换为 LogicExpression 以处理逻辑连接 (AND / OR)
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(predicate.get())) {
    if (logic_expr->logic_type_ == LogicType::And) {
      // 如果是 AND 表达式，则递归处理左右子表达式
      ExtractJoinKeys(logic_expr->GetChildAt(0), left_keys, right_keys);
      ExtractJoinKeys(logic_expr->GetChildAt(1), left_keys, right_keys);
    }
  } else if (const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(predicate.get())) {
    // 检查是否为等值比较 (==)
    if (comparison_expr->comp_type_ == ComparisonType::Equal) {
      const auto left_expr = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(0));
      const auto right_expr = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(1));

      if (left_expr != nullptr && right_expr != nullptr) {
        // 确保左右表的列正确匹配
        if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
          left_keys.push_back(left_expr);
          right_keys.push_back(right_expr);
        } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
          left_keys.push_back(right_expr);
          right_keys.push_back(left_expr);
        }
      }
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 递归优化子计划
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  LOG_DEBUG("called");

  // 检查是否为 NestedLoopJoinPlanNode
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // 确保它有两个子节点
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    // 定义左右连接键的向量
    std::vector<AbstractExpressionRef> left_keys;
    std::vector<AbstractExpressionRef> right_keys;

    // 检查是否为 AND 连接的多个等值比较
    ExtractJoinKeys(nlj_plan.Predicate(), left_keys, right_keys);

    // 如果有多个连接键，则将它们传递给 HashJoinPlanNode
    if (!left_keys.empty() && !right_keys.empty()) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_keys, right_keys, nlj_plan.GetJoinType());
    }
  }

  // 如果不能优化，则返回优化后的计划
  return optimized_plan;
}

}  // namespace bustub
