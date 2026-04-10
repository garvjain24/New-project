# Pipeline Analysis Report

Generated at: 2026-04-10 18:47:58

## System

- Uptime seconds: 77.44
- Crash-free uptime seconds: 77.44
- Crash count: 0
- Successful batches: 6
- Failed batches: 0
- Batch success rate: 100.0%

## Agent Metrics

- LLM calls: 0
- LLM successful suggestions: 0
- Local similarity routes: 21
- Local fallback routes: 10
- LLM guided routes: 0
- Local rule resolutions: 25
- LLM assisted resolutions: 0

## Human Metrics

- Human approval requests: 4
- Human full-resolution requests: 2
- Human approvals completed: 2
- Human rejections: 0
- Manual fixes: 0
- Open human queue: 2

## Performance

- MTTD: 5.55s
- MTTR: 41.48s
- Issues detected total: 31
- Issues resolved total: 27
- Issues escalated total: 4

## Recent Batches

- `batch-006` | status=attention | size=13 | detected=4 | resolved=2 | escalated=2
- `batch-005` | status=degraded | size=10 | detected=4 | resolved=4 | escalated=0
- `batch-004` | status=attention | size=13 | detected=7 | resolved=6 | escalated=1
- `batch-003` | status=degraded | size=14 | detected=6 | resolved=6 | escalated=0
- `batch-002` | status=healthy | size=11 | detected=6 | resolved=6 | escalated=0
- `batch-001` | status=attention | size=14 | detected=4 | resolved=3 | escalated=1

## Open Escalations

- `batch-004-001` | batch=batch-004 | dataset=payments | status=pending | reason=Restore blank primary key from clean source trace.
- `batch-006-001` | batch=batch-006 | dataset=payments | status=pending | reason=Restore blank primary key from clean source trace.
