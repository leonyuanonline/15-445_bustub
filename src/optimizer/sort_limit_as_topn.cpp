#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 1. 递归优化所有子计划
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    LOG_DEBUG("Optimizing child plan.");
    auto optimized_child = OptimizeSortLimitAsTopN(child);
    children.push_back(optimized_child);
  }

  // 2. 处理 LimitPlanNode
  if (plan->GetType() == PlanType::Limit) {
    LOG_DEBUG("Plan is a LimitPlanNode.");

    // 获取 LimitPlanNode 并检查子节点是否为 SortPlanNode
    const auto *limit_plan = dynamic_cast<const LimitPlanNode *>(plan.get());

    // 使用递归优化后的子计划
    const auto &optimized_child_plan = children[0];  // 使用递归优化后的子计划

    if (optimized_child_plan->GetType() == PlanType::Sort) {
      LOG_DEBUG("Child plan is a SortPlanNode.");

      const auto *sort_plan = dynamic_cast<const SortPlanNode *>(optimized_child_plan.get());

      // 获取排序条件和 Limit 的 N
      auto order_bys = sort_plan->GetOrderBy();
      size_t limit_n = limit_plan->GetLimit();

      // 构建新的 TopNPlanNode
      auto topn_plan = std::make_shared<TopNPlanNode>(std::make_shared<Schema>(plan->OutputSchema()),
                                                      sort_plan->GetChildPlan(), order_bys, limit_n);

      LOG_DEBUG("Optimized to TopNPlanNode.");
      return topn_plan;
    }

    // 如果优化后的子计划已经是 TopNPlanNode，直接返回计划
    if (optimized_child_plan->GetType() == PlanType::TopN) {
      LOG_DEBUG("Plan is already a TopNPlanNode. Skipping.");
      return optimized_child_plan;
    }
  }

  // 3. 返回优化后的计划节点
  if (!children.empty()) {
    // 重新构建计划节点，保持原计划结构不变，替换子节点
    return plan->CloneWithChildren(children);
  }

  return plan;
}

}  // namespace bustub
