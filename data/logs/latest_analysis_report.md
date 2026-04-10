# Pipeline Analysis Report

Generated at: 2026-04-11 02:01:27

## System

- Uptime seconds: 131.4
- Crash-free uptime seconds: 131.4
- Crash count: 0
- Successful batches: 6
- Failed batches: 0
- Batch success rate: 100.0%

## Agent Metrics

- LLM calls: 31
- LLM successful suggestions: 0
- Local similarity routes: 0
- Local fallback routes: 31
- LLM guided routes: 0
- Local rule resolutions: 27
- LLM assisted resolutions: 0

## Human Metrics

- Human approval requests: 5
- Human full-resolution requests: 2
- Human approvals completed: 0
- Human rejections: 0
- Manual fixes: 0
- Open human queue: 5

## Performance

- MTTD: 5.24s
- MTTR: 40.62s
- Issues detected total: 34
- Issues resolved total: 27
- Issues escalated total: 7

## Recent Batches

- `batch-006` | status=attention | size=14 | detected=8 | resolved=7 | escalated=1
- `batch-005` | status=attention | size=15 | detected=6 | resolved=4 | escalated=2
- `batch-004` | status=degraded | size=10 | detected=4 | resolved=4 | escalated=0
- `batch-003` | status=attention | size=13 | detected=4 | resolved=3 | escalated=1
- `batch-002` | status=attention | size=13 | detected=6 | resolved=5 | escalated=1
- `batch-001` | status=attention | size=14 | detected=6 | resolved=4 | escalated=2

## Open Escalations

- `batch-001-004` | batch=batch-001 | dataset=delivery | status=pending | reason=Restore blank primary key from clean source trace.
- `batch-002-006` | batch=batch-002 | dataset=orders | status=pending | reason=Restore blank primary key from clean source trace.
- `batch-003-002` | batch=batch-003 | dataset=orders | status=pending | reason=Restore blank primary key from clean source trace.
- `batch-005-004` | batch=batch-005 | dataset=orders | status=pending | reason=Restore blank primary key from clean source trace.
- `batch-006-008` | batch=batch-006 | dataset=delivery | status=pending | reason=Restore blank primary key from clean source trace.
