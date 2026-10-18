-- Circuit breaker for the decision model (triage after the prefilter, providers/decision.ts).
INSERT INTO budgets (service, per_minute, per_hour, per_day, note) VALUES
  ('decision', 60, 1000, 3000, '决策模型熔断（精选前的价值初筛 triage）')
ON CONFLICT (service) DO NOTHING;
