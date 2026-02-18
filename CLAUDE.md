# @stravigor/queue

Background job processing and task scheduling for the Strav framework.

## Dependencies
- @stravigor/kernel (peer)
- @stravigor/database (peer)

## Consumed by
- @stravigor/signal (for queued mail/notifications)

## Commands
- bun test
- bun run typecheck

## Architecture
- src/queue/ — Queue manager, worker, job dispatching
- src/scheduler/ — Task scheduler with cron expressions
- src/providers/ — QueueProvider

## Conventions
- Jobs are stored in the database via @stravigor/database
- Scheduler runs standalone or via CLI (`strav scheduler:work`)
