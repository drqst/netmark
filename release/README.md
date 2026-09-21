# netmark

CLI load testing tool. One binary is both the sending and the receiving side:
start it on two hosts, make one a server and the other a client, and it pushes
traffic between them, measures what arrives, reconciles the two sides against
each other and records the result.

**Full documentation: [doc/netmark.md](doc/netmark.md).**
**REST API contract: [doc/openapi.yaml](doc/openapi.yaml).**

## Quick start

```sh
cargo build --release
./target/release/netmark                             # interactive; type `help`
./target/release/netmark profiles/udp-10kbps.yaml    # one run, exit 0 or 1
./init.sh                                            # Kubernetes: database + app with web CLI
./stop.sh                                            # stop all pods; the postgres data volume is kept
./test.sh                                            # test init.sh/stop.sh and the volume persistence
```

## Linux Release Bundle

Build a portable `release/` directory for another Linux host with:

```sh
./package-release.sh
scp -r release user@other-linux:/opt/netmark
ssh user@other-linux 'cd /opt/netmark && ./netmark'
```

The directory contains `netmark` and `netmarkctl` side by side, plus the
profiles, configuration, lifecycle scripts, Kubernetes manifests, and source
build context required by `./init.sh`. On the target host, run `./check.sh` to
see required Docker and Kubernetes prerequisites, then run `./init.sh` or use
the binary directly:

```sh
cd /opt/netmark
./netmark profiles/udp-10kbps.yaml
./netmarkctl help
```

## Kubernetes Metrics

`./init.sh` installs kind automatically on x86_64 Linux when it is missing,
builds and loads the local application image, and returns to the prompt after
PostgreSQL, netmark and Grafana are ready. PostgreSQL remains available through
a background port-forward at `127.0.0.1:5433`; its log is
`/tmp/netmark-postgres-port-forward.log`. `./stop.sh` removes the workloads,
stops that port-forward, and returns after its bounded PostgreSQL shutdown wait.
The database volume remains in place for the next startup.

The local PostgreSQL and Grafana administrator credentials are `admin` /
`password`; the database is `netmark`. Add other database accounts after
startup when needed:

```sh
kubectl -n netmark exec deployment/netmark-postgres -c postgres -- \
  psql -U admin -d netmark -c "CREATE ROLE analyst LOGIN PASSWORD 'change-me';"
```

Every completed run creates one `netmark_metrics` row. Its `protocol` column
separates `tcp`, `sctp`, `udp`, and `ip` runs in the shared table. UDP sequence
numbers are written to `lost_udp_packets` and `out_of_order_udp_packets` for
each run. Grafana at <http://127.0.0.1:3000> includes TCP, SCTP, UDP and raw-IP
panels that query these protocol-specific rows.

```sh
kubectl -n netmark exec deployment/netmark-postgres -c postgres -- \
  psql -U admin -d netmark -c \
  "SELECT run_id, lost_udp_packets, out_of_order_udp_packets
   FROM netmark_metrics WHERE protocol = 'udp' ORDER BY timestamp_utc DESC;"
```

```sh
./init.sh
./netmarkctl run profiles/udp-10kbps.yaml
curl -u admin:password http://127.0.0.1:3000/api/dashboards/uid/netmark-metrics-dash
./k8s/cluster-test.sh
./stop.sh
```

## What it does

- **Transports:** TCP, UDP, and raw IPv4 (protocol 253, needs `CAP_NET_RAW`).
- **WebRTC layer:** optional data-channel framing on top of any transport.
- **Multiple clients:** each with its own id starting at 0, its own destination,
  and optional runtime and jitter overrides.
- **Debrief:** at the end of every run the client and server reconcile packets
  and bytes; anything that does not match exactly fails the run.
- **Bandwidth up and down** on every run, in stdout, the logs, the local SQLite
  database and the external one.
- **Monitoring:** periodic HTTP checks with alarms, plus an SMTP connection check.
- **REST API and Rust SDK** over the same code path as the CLI.
- **Kubernetes deployment:** `init.sh` brings up a kind cluster with a database
  node (PostgreSQL/Timescale on a persistent volume), an app node serving a
  **web interface with a web CLI**, and **Grafana** graphing the external metrics
  database (every point tagged with `run_id`); `./netmarkctl` controls the app
  from the shell. `stop.sh` stops and removes every pod but keeps the data
  volume, so PostgreSQL data persists between runs; `test.sh` tests all of it.

## Defaults

Nothing leaves the host unless you ask for it. The external metrics database, the
REST API, SMTP and the WebRTC layer are all off until enabled, and the REST API
binds to loopback when you do enable it.

## Running across many machines

The usual shape is one server machine and many client machines — one receiver and
ten senders, say. Run **one netmark per machine**; to load a sender harder, add
more clients to its profile rather than more processes. Two servers cannot share
a machine, because they would both want ports 9000 and 9001.

Every profile takes a `start_at`, an RFC 3339 UTC instant. netmark sets the run up
and then blocks until that moment before sending, so handing all eleven machines
the same value starts them together — keep their clocks in step with `chrony` or
`ntpd` and pick a moment far enough ahead to cover startup.

```sh
AT=$(date -u -d '+30 seconds' +%Y-%m-%dT%H:%M:%SZ)
for host in receiver sender-{1..10}; do
  ssh "$host" "sed -i 's|^start_at:.*|start_at: \"$AT\"|' /opt/netmark/profile.yaml \
               && /opt/netmark/netmark /opt/netmark/profile.yaml" &
done
wait
```

Get the profile onto each machine either **preloaded** — shipped with the binary
and run as `netmark profile.yaml`, with nothing listening and nothing to secure —
or **pushed** by a controller that POSTs it to each machine's REST API and
collects the reports. [doc/netmark.md](doc/netmark.md#running-across-many-machines)
has both, with a worked controller example and how to collect results afterwards.

## Plans

- Send email when the monitor fails.
- More traffic patterns and protocols.
- A better status page.
