# Pipeline Analysis Report

Generated at: 2026-04-10 21:34:04

## System

- Uptime seconds: 283.88
- Crash-free uptime seconds: 283.88
- Crash count: 0
- Successful batches: 6
- Failed batches: 0
- Batch success rate: 100.0%

## Agent Metrics

- LLM calls: 0
- LLM successful suggestions: 0
- Local similarity routes: 28
- Local fallback routes: 9
- LLM guided routes: 0
- Local rule resolutions: 29
- LLM assisted resolutions: 0

## Human Metrics

- Human approval requests: 5
- Human full-resolution requests: 3
- Human approvals completed: 3
- Human rejections: 0
- Manual fixes: 0
- Open human queue: 2

## Performance

- MTTD: 5.46s
- MTTR: 45.54s
- Issues detected total: 37
- Issues resolved total: 32
- Issues escalated total: 5

## Recent Batches

- `batch-006` | status=degraded | size=15 | detected=5 | resolved=5 | escalated=0
- `batch-005` | status=attention | size=15 | detected=4 | resolved=3 | escalated=1
- `batch-004` | status=attention | size=13 | detected=8 | resolved=7 | escalated=1
- `batch-003` | status=attention | size=12 | detected=8 | resolved=7 | escalated=1
- `batch-002` | status=attention | size=13 | detected=5 | resolved=4 | escalated=1
- `batch-001` | status=attention | size=12 | detected=7 | resolved=6 | escalated=1

## Open Escalations

- `batch-004-002` | batch=batch-004 | dataset=delivery | status=pending | reason=Restore blank primary key from clean source trace.
- `batch-005-004` | batch=batch-005 | dataset=delivery | status=pending | reason=Restore blank primary key from clean source trace.
