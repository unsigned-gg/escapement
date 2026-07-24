# Blackwall task — EST-17: DAG plan types + topological resolver
#
# Run: blackwall run start --task est-17-dag-plans --provider gateway
#
# Adds DAG plan types (Plan, PlanNode, PlanEdge) to escapement-core with
# topological resolution, cycle detection, missing-needs validation, and
# duplicate-id check. Zero new dependencies.

[task]
name = "est-17-dag-plans"
provider = "gateway"
model = "llm/glm-5.2"
prompt = """Implement Linear ticket EST-17: add DAG plan types and topological resolver to escapement-core.

Create a new module `plan` in `crates/escapement-core/src/plan.rs` with:

1. `PlanNode` — wraps a `Task` (from dispatch.rs) plus a `BTreeSet<PlanNodeId>` of dependency task ids (the `needs` set). Builder-style: `PlanNode::new(task)` and `.with_needs(ids)`.

2. `Plan` — a validated DAG of `PlanNode`s, stored in a `BTreeMap<PlanNodeId, PlanNode>` for deterministic iteration. API: `from_nodes(iter) -> Result<Plan, PlanError>`, `add_node(node) -> Result<(), PlanError>` (duplicate check), `validate() -> Result<(), PlanError>`, `topological_order() -> Vec<&PlanNode>` (Kahn's algorithm, sorted ready set for determinism).

3. `PlanError` enum: `DuplicateNode(PlanNodeId)`, `MissingNeed { node, missing }`, `Cycle(PlanNodeId)`. Implements `std::error::Error`.

Validation:
- Duplicate ids rejected at insertion time
- Missing needs (references to non-existent node ids) rejected
- Cycles detected via DFS with a recursion stack
- Self-cycles rejected (node that needs itself)

Topological resolution:
- Dependencies before dependents (Kahn's algorithm)
- Nodes with no remaining unmet deps emitted in task-id order (deterministic: BTreeSet ready set)
- Nodes in a cycle are omitted (shouldn't happen on a validated plan)

Re-export from `lib.rs`: `pub mod plan;` plus `pub use plan::{Plan, PlanNode, PlanError};` and `pub type PlanNodeId = ...`.

Write comprehensive unit tests in `#[cfg(test)] mod tests`:
- rejects_duplicate_node
- rejects_missing_need
- rejects_cycle_two_nodes (a needs b, b needs a)
- rejects_cycle_three_nodes (a→b→c→a)
- rejects_self_cycle
- valid_plan_resolves_topologically (diamond: a→b,c→d, verify ordering)
- topological_order_is_deterministic (two independent nodes, verify id-sorted)
- single_node_plan
- empty_plan
- diamond_dependency_resolves

Constraints:
- Zero new dependencies in escapement-core (std only)
- `cargo fmt --check --all` clean
- `cargo clippy --all-targets -- -D warnings` clean (workspace has pedantic lints)
- `cargo test --all-targets` all pass (existing tests + new tests)
- Do NOT modify existing files (lib.rs, dispatch.rs, registry.rs) except to add the `pub mod plan;` and re-exports to lib.rs"""
