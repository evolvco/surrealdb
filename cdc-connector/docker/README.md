# Local CDC test harness

This docker-compose spins up a minimal TiKV + SurrealDB + cdc-connector
stack for end-to-end testing of the CDC pipeline.

## Prerequisites

- Docker Desktop (macOS/Linux/Windows)
- `surreal` CLI on your host (optional — for easy interaction)

## Start the stack

```bash
docker compose -f docker-compose.cdc-test.yaml up --build
```

First run takes ~15 minutes to build SurrealDB from source. Subsequent
runs are instant (Docker caches the build layers).

Wait until you see:

- `pd` log: `entering normal stage`
- `tikv` log: `Welcome to TiKV`
- `surrealdb` log: `Started web server on 0.0.0.0:8000`
- `cdc-connector` log: `installing subscription` followed by no error messages

## Run the smoke test

In one terminal, start a LIVE SELECT:

```bash
surreal sql --conn http://localhost:8000 \
  --user root --pass root \
  --ns test --db test
```

At the SurrealDB prompt:

```sql
LIVE SELECT * FROM person;
```

The prompt will block waiting for events.

In a second terminal, write a record:

```bash
surreal sql --conn http://localhost:8000 \
  --user root --pass root \
  --ns test --db test \
  --pretty
```

```sql
CREATE person:alice SET name = "Alice", age = 30;
UPDATE person:alice SET age = 31;
DELETE person:alice;
```

Each operation should produce a notification in the first terminal
within a second or two. If notifications fire, the CDC pipeline is
working end-to-end.

## Shut down and clean up

```bash
docker compose -f docker-compose.cdc-test.yaml down -v
```

The `-v` flag also removes the TiKV/PD data volumes so the next run
starts clean.

## Debugging

- **Connector can't reach PD**: check `docker compose logs pd` — PD needs
  to be healthy before the connector subscribes.
- **No notifications fire**: check `docker compose logs cdc-connector` for
  stream errors; check `docker compose logs surrealdb` for CBOR decode
  failures.
- **SurrealDB rejects the connector's ingest request**: make sure
  `CDC_SURREAL_USERNAME` / `CDC_SURREAL_PASSWORD` match what SurrealDB
  was started with (both `root` in the compose file).
