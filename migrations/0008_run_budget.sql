-- migrations/0008_run_budget.sql
ALTER TABLE agent_runs ADD COLUMN budget_json TEXT
    CHECK (budget_json IS NULL OR json_valid(budget_json));
