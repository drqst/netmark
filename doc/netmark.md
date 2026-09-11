# netmark

A network load generator and monitor. One binary is both the sending side and the
receiving side: start it on two hosts, make one a server and the other a client,
and it will push traffic between them, measure what arrives, reconcile the two
sides against each other and record the result.

- [How it works](#how-it-works)
- [Getting started](#getting-started)
- [The interactive CLI](#the-interactive-cli)
- [Command reference](#command-reference)
- [Test profiles](#test-profiles)
- [Transports](#transports)
- [The WebRTC layer](#the-webrtc-layer)
- [Bandwidth reporting](#bandwidth-reporting)
- [The debrief](#the-debrief)
- [Where data is stored](#where-data-is-stored)
- [Monitoring and alerts](#monitoring-and-alerts)
- [The REST API](#the-rest-api)
- [Using netmark as a Rust library](#using-netmark-as-a-rust-library)
- [Ports and permissions](#ports-and-permissions)

---

## How it works

A **run** is one measured burst of traffic. It has an id, a start and an end, and
a verdict of `ok` or `fail`.

```
   client host                                    server host
  ┌───────────────────────┐                     ┌───────────────────────┐
  │ client 0 ─┐           │   TCP / UDP / IP    │                       │
  │ client 1 ─┼─► traffic ├────────────────────►│ server ──► counters   │
  │ client N ─┘           │                     │                       │
  │                       │   debrief (9001)    │                       │
  │ counters  ◄───────────┼────────────────────►│                       │
  └──────────┬────────────┘                     └───────────┬───────────┘
             │                                              │
      local SQLite + logs                            local SQLite + logs
             │                                              │
             └──────────► optional external SQL ◄───────────┘
```

Both sides count packets and bytes independently. When the traffic stops, the
client tells the server what it put on the wire and asks what arrived; both sides
write that reconciliation to their own database and log. If the numbers do not
match exactly, the run fails.

Every run also produces a **bandwidth** figure in both directions — upload from
the sending side and download on the receiving side — which is printed, logged and
stored.

A netmark instance can drive several **clients** at once. Each has an id starting
at 0, its own destination, and optional runtime and jitter overrides.

Two ways to drive it:

- **Interactive**, by running `netmark` with no arguments.
- **Non-interactive**, by running `netmark <profile.yaml>`, which executes one
  test profile, prints the result and exits with status 0 on pass and 1 on fail.
  This is the form to put in CI.

There is also a REST API and a Rust SDK; both go through the same code path.

---

## Getting started

Build it:

```sh
cargo build --release
```

Check that it works on one machine — this sends traffic to localhost over every
protocol in turn, three seconds each, and reports the result of each one:

```sh
./target/release/netmark
> selftest
```

Run one of the shipped profiles without the CLI:

```sh
./target/release/netmark profiles/udp-10kbps.yaml
```

```
auto mode: started run 255
auto mode: debrief run=255 role=client protocol=udp sent_packets=50 received_packets=50 sent_bytes=51200 received_bytes=51200 lost=0 out_of_order=0 result=match
auto mode: run 255 result=ok sent_bytes=51200 received_bytes=51200 up=17066 bytes/sec down=17066 bytes/sec
```

### Between two machines

On the receiving host:

```
> server enable
> start
```

On the sending host:

```
> client 0 remote 10.0.0.7
> client 0 enable
> start
...
> stop
```

---

## The interactive CLI

Run `netmark` with no arguments. The prompt shows which roles are active:

```
Client | Server >
```

- **`help`** lists every command. `help <command>` narrows it to one command and
  its subcommands, in the same words the command itself uses: `help start`,
  `help configure tcp`, `help client 0 remote`, `help configure limits udp`.
  `help sctp` opens the detailed SCTP page.
- **Up and down arrows** walk the command history. Position 0 is the line you are
  typing; pressing up moves back through earlier commands and pressing down comes
  forward again, returning your unfinished line when you reach position 0.
- **Tab** switches between the command prompt and the live traffic display.
- **Esc** stops a running test.
- **Ctrl-C** or `exit` shuts down.

While a run is in progress the live display prints one line per second:

```
up 10240 bytes/sec, down 10240 bytes/sec | Client sent: TCP 0 bytes, UDP/IP 10240 bytes | Server received: TCP 0 bytes, UDP/IP 10240 bytes (lost 0, out-of-order 0, jitter 1 ms)
```

`status` prints a table of everything the instance is doing:

```
Traffic           running (udp)
Run               42 (7 s elapsed)
Bandwidth up      10240 bytes/sec
Bandwidth down    10240 bytes/sec
Sent              71680 bytes  (TCP/SCTP 0, UDP 71680, IP 0)
Received          71680 bytes  (TCP/SCTP 0, UDP 71680, IP 0)
UDP loss          0 lost, 0 out of order
Jitter            TCP 0 ms, UDP 2 ms
Server            enabled
Client 0          client 0 enabled remote=10.0.0.7 webrtc=follow
Client 1          client 1 disabled remote=10.0.0.8 webrtc=off
WebRTC            webrtc disabled channels=1 label=netmark ordered=true
Monitor           on id 1, 12 calls, 12 ok, 0 failed
Metrics SQL       not connected
REST API          disabled
Web server        listening on http://127.0.0.1:8081 (port 8081)
SCTP              not selected (available)
SMTP              disabled
```

The same web server line is printed when `./netmark` starts, so the port the web
interface and REST API listen on is visible without opening the configuration.

---

## Command reference

Everything the prompt accepts. `help` prints the same list, and `help <command>`
prints just the rows for that command.

### Roles

| Command | Effect |
| --- | --- |
| `server enable` / `server disable` | Turn the receiving side on or off |
| `server runtime <seconds>` | Stop the server after N seconds; 0 is unlimited |
| `client list` | Show every client and its settings |
| `client add` | Add a client using the lowest free id |
| `client delete <id>` | Remove a client |
| `client <id> enable` / `client <id> disable` | Turn one client on or off |
| `client <id> remote <ip>` | Set that client's destination |
| `client <id> runtime <seconds>` | Per-client runtime override |
| `client <id> jitter <ms>` | Per-client send jitter |
| `client <id> webrtc <on\|off\|follow>` | Connect the WebRTC layer to that client alone |
| `client <id> status` | Show one client's settings |
| `client <id> http check <url>` | Fetch one HTTP or HTTPS page and time it |

`<id>` starts at 0. Leaving it out is shorthand for client 0, so `client enable`
still works.

### Traffic settings

| Command | Effect |
| --- | --- |
| `configure type <tcp\|sctp\|udp\|ip>` | Pick the transport |
| `configure tcp bytes <bytes/sec>` | TCP bytes per second |
| `configure tcp window <bytes>` | TCP send/receive window; 0 uses the OS default |
| `configure tcp jitter <ms>` | Deliberate jitter added to TCP sends |
| `configure tcp maxjitter <ms>` | Fail the run above this measured TCP jitter |
| `configure udp_rate <packets/sec>` | UDP packets per second; also paces raw IP |
| `configure udp packetsize <bytes>` | UDP (and raw IP) packet size |
| `configure udp jitter <ms>` | Deliberate jitter added to UDP sends |
| `configure udp max jitter <ms>` | Fail the run above this measured UDP jitter |
| `configure bandwidth limit <bytes/sec>` | Minimum acceptable throughput; 0 disables |
| `configure limits` | Show every per-protocol limit that can fail a run |
| `configure limits <tcp\|sctp\|udp\|ip> status` | Show one protocol's limits |
| `configure limits <tcp\|sctp\|udp\|ip> <parameter> <value>` | Set a limit; 0 removes it |
| `configure limits <tcp\|sctp\|udp\|ip> clear` | Remove every limit for one protocol |
| `configure protocols` | Every transport: enabled or disabled, which is selected, host support and WebRTC |
| `configure <tcp\|sctp\|udp\|ip> enable` | Allow the transport to be selected, started and selftested |
| `configure <tcp\|sctp\|udp\|ip> disable` | Turn the transport off; `start` refuses it, `selftest` skips it and selecting it is an error |
| `configure save` | Write the current settings to netmark.config |
| `configure reset` | Reload netmark.config, discarding session changes |

#### Per-protocol limits

`configure limits` decides when a run fails, per transport, so the same limit
never has to mean two things. `help configure limits tcp` lists the parameters
one protocol accepts:

| Parameter | Protocols | Fails the run when |
| --- | --- | --- |
| `min-sent-bytes` | all | fewer bytes were sent in the run |
| `min-received-bytes` | all | fewer bytes were received in the run |
| `min-sent-bytes-per-second` | all | the sending throughput was lower |
| `min-received-bytes-per-second` | all | the receiving throughput was lower |
| `max-jitter-millis` | tcp, sctp, udp | the measured jitter was higher |
| `max-lost-packets` | udp | more packets were lost |
| `max-out-of-order-packets` | udp | more packets arrived out of order |

A value of 0 means the limit is not checked, which is the default for all of
them. Only the limits of the transport a run used are applied, and every limit
that was missed is named in the run's failure reason. `configure save` writes
them to `netmark.config` under `traffic.limits`.

```
> configure limits udp max-lost-packets 5
udp max-lost-packets limit set to 5
> configure limits udp status
udp min-sent-bytes                       not set
udp min-received-bytes                   not set
udp min-sent-bytes-per-second            not set
udp min-received-bytes-per-second        not set
udp max-jitter-millis                    not set
udp max-lost-packets                     5
udp max-out-of-order-packets             not set
```

#### Turning protocols on and off

Every transport is enabled by default. `configure <protocol> disable` takes one
out of use: it cannot be selected with `configure type`, `start` refuses to run
it and `selftest` skips it with the reason. Disabling the selected transport
moves the selection to an enabled one. The state is shown by `configure
protocols` and in the `Protocols` row of `status`, and `configure save` writes it
to `netmark.config` under `traffic.protocols`.

SCTP and WebRTC are configured the same way, as subcommands of `configure`:
`configure sctp` opens the detailed SCTP page, `configure sctp status` reports
kernel support, and `configure webrtc ...` holds the data-channel settings.

### WebRTC

| Command | Effect |
| --- | --- |
| `configure webrtc enable` / `configure webrtc disable` | Turn the data-channel layer on or off for every client that follows |
| `configure webrtc channels <n>` | Spread messages over N channels |
| `configure webrtc label <name>` | Data-channel label |
| `configure webrtc ordered <true\|false>` | Ordered or unordered delivery |
| `configure webrtc status` | Show the current settings |
| `client <id> webrtc <on\|off\|follow>` | Override the layer for one client |

**Connecting WebRTC to a particular client.** The `webrtc` command sets the
layer's settings and its default on/off state. Each client has its own switch,
which starts at `follow`, meaning it takes whatever `webrtc enable` /
`webrtc disable` says. Setting `client <id> webrtc on` or `off` pins that one
client regardless. That is how you run data-channel traffic and plain traffic
side by side from the same machine:

```
> webrtc channels 4
> webrtc disable          # plain traffic is the default
> client 0 webrtc on      # client 0 sends data channels
> client 1 webrtc off     # client 1 stays plain
> client 2                # left on `follow`, so plain
> start
```

In a profile the same switch is the `webrtc` field on each client:

```yaml
webrtc:
  enabled: false
  channels: 4
clients:
  - id: 0
    enabled: true
    remote: 10.0.0.7
    webrtc: true      # true | false | null (follow)
```

### Running

| Command | Effect |
| --- | --- |
| `start` / `stop` | Start and stop a run; `stop` names the transport it stopped, ends an SCTP association and debriefs over the same transport |
| `configure sctp` | Detailed SCTP help: kernel support, how to select, run and stop it |
| `configure sctp status` | Whether the kernel can open an SCTP socket |
| `selftest` | Three seconds to localhost over udp, tcp, sctp and ip in turn, each start to verdict; a transport this host cannot carry is skipped with the reason |
| `benchmark duration <seconds>` | Flood the remote with TCP and report bandwidth |
| `status` | Current counters, bandwidth up and down, and the state of everything else, as a table |
| `list` | Every run with its role, transport, result, bandwidth and per-protocol packet, byte, loss and jitter counters |
| `show run <id>` | Everything stored for one run, as a table: times, role, transport, result and failure reason, totals and bandwidth, the TCP/SCTP, UDP and raw IP packet and byte counters, loss, jitter, the TCP transport figures and the WebRTC counts, followed by both sides' debriefs |
| `clean` | Delete stored data, keeping the run id counter |

### Metrics, monitoring and administration

| Command | Effect |
| --- | --- |
| `configure metrics <connection>` | Set the external SQL target |
| `metrics enable` / `metrics disable` / `metrics status` | Control external SQL |
| `monitor IP <url>` | Set the HTTP or HTTPS monitor target |
| `monitor start` / `monitor stop` | Start or stop 30-second checks |
| `monitor history` | Show monitor events and alarms |
| `admin add email <address>` / `admin delete email <address>` | Administrator addresses |
| `configure smtp <host[:port]>` | SMTP server used for alerts |
| `admin smtp enabled` / `admin smtp disabled` | Turn SMTP on or off |
| `admin smtp status` | Check the connection to the SMTP server |
| `restapi enable [<address>]` / `restapi disable` / `restapi status` | Control the REST API |

---

## Test profiles

A profile is a self-contained run in YAML. `profiles/` has working examples.

```yaml
server:
  enabled: true
clients:
  - id: 0
    enabled: true
    remote: 127.0.0.1
  - id: 1
    enabled: true
    remote: 127.0.0.1
    runtime: 2          # optional per-client override
    jitter_millis: 5    # optional per-client override
    webrtc: null        # true | false | null to follow the webrtc section
traffic:
  udp_rate: 10                 # UDP packets per second; also paces raw IP
  packet_type: udp             # tcp | sctp | udp | ip
  tcp_bytes_per_second: 10240
  tcp_window_size: 0            # 0 uses the OS default
  udp_packet_size: 1024
  client_runtime: 3
  server_runtime: 3
  jitter_millis: 0
  client_jitter_millis: 0
  server_jitter_millis: 0
  max_tcp_jitter_millis: 1000  # a run fails above this measured jitter
  max_udp_jitter_millis: 1000
  limit: 0                     # minimum acceptable bytes/sec; 0 disables
webrtc:
  enabled: false
  channels: 1
  label: netmark
  ordered: true
metrics:
  sql: null                    # external database; null means nothing leaves this host
duration_seconds: 3
start_at: null                 # RFC 3339 UTC instant to begin at; null starts now
hooks:                         # names of Rust callbacks; see the SDK section
  before: []
  after: []
```

Run it:

```sh
netmark profiles/udp-10kbps.yaml
echo $?    # 0 = pass, 1 = fail
```

---

## Transports

`configure type` and `traffic.packet_type` select one of four.

### TCP

A byte stream paced to `tcp_bytes_per_second`. A send timestamp is embedded every
100 bytes so the receiver can measure jitter on a stream that has no packet
boundaries. Those timestamped chunks are what both sides count as packets, so the
two counts line up exactly.

### SCTP

A reliable, message-oriented SCTP association paced by `tcp_bytes_per_second`.
The host kernel must support SCTP; netmark reports the socket error if it does
not. SCTP shares the stream framing and TCP jitter budget, while TCP-only MSS,
MTU and window values remain zero for SCTP runs. `stop` ends the association so
the receiving side sees the end of the run at once. The `sctp` command prints a
detailed help page covering all of that, including whether this kernel supports
SCTP; it is available in the web CLI too.

### UDP

One datagram per send, at `udp_rate` packets per second and `udp_packet_size`
bytes each. Each packet carries a 24-byte header: sequence number, send timestamp
and client id. Sequence numbers are per client, so several clients sending at one
server are not mistaken for reordering.

### Raw IP

The same payload carried directly in IPv4 packets with no transport header at
all, using IP protocol number 253 (reserved for experimentation by RFC 3692).
This measures the path without TCP's congestion control or UDP's checksum, which
is useful for isolating whether a problem is in the network or in the transport.

Opening a raw socket requires `CAP_NET_RAW`. Either run as root:

```sh
sudo ./target/release/netmark profiles/ip.yaml
```

or grant the capability once:

```sh
sudo setcap cap_net_raw+ep ./target/release/netmark
```

Without it, netmark says so plainly and the run fails rather than reporting a
misleading zero.

---

## The WebRTC layer

`webrtc enable` wraps the payload of whichever transport is selected in WebRTC
data-channel frames: DCEP-style channel setup, a per-channel message sequence,
and an ordered/unordered flag. Messages are spread round-robin over
`webrtc channels` channels. The receiver decodes the frames, counts messages per
channel and flags gaps and reordering per channel.

This is **not** a full WebRTC stack. There is no ICE, DTLS or SCTP association,
because netmark measures a network path rather than interoperating with a
browser. What it reproduces is the part that shapes the traffic and the failure
modes: channel multiplexing and per-channel ordering. If you need to test against
a real browser peer, this is not that.

The frame header is 16 bytes and sits inside the transport payload, so byte
accounting and the debrief are unaffected.

---

## Running across many machines

The usual shape of a large test is **one server machine and many client
machines** — say one receiver and ten senders, all aimed at it. The binary is the
same everywhere; only the profile differs.

### Lay it out

Give the receiver a profile with the server on and no clients:

```yaml
# server.yaml, deployed to the one receiving machine
server:
  enabled: true
clients: []
traffic:
  packet_type: udp
  server_runtime: 60
duration_seconds: 60
```

Give every sender the same client profile, pointed at the receiver:

```yaml
# client.yaml, deployed to all ten sending machines
server:
  enabled: false
clients:
  - id: 0
    enabled: true
    remote: 10.0.0.7
traffic:
  packet_type: udp
  udp_rate: 500
  udp_packet_size: 1024
  client_runtime: 60
duration_seconds: 60
```

Run one netmark per machine. To put more load on a sender than one thread can
produce, add more **clients** to that machine's profile rather than more
processes; they share one set of counters and one debrief, and each is
distinguishable by its id in `client.log`.

Do not run two netmark servers on one machine: they would both want TCP and UDP
port 9000 and the debrief port 9001.

### Start the receiver first

Only the receiver needs a head start; a TCP client retries its connect, and UDP
and raw IP senders do not connect at all, so anything sent before the server is
listening is simply lost and shows up as a debrief mismatch. Give it a few
seconds, or use `start_at` below, which handles both sides.

### Starting everything at the same instant

Every profile takes an optional `start_at`: an RFC 3339 UTC instant. netmark sets
its run up, then blocks until that moment before it puts a byte on the wire. Hand
all eleven machines the same value and they begin together, to the accuracy of
their clocks — so run `chrony` or `ntpd` on all of them first, and pick a moment
far enough ahead to cover deployment and startup.

```yaml
duration_seconds: 60
start_at: "2026-08-28T15:00:00Z"
```

```sh
AT=$(date -u -d '+30 seconds' +%Y-%m-%dT%H:%M:%SZ)
for host in receiver sender-{1..10}; do
  ssh "$host" "sed -i 's|^start_at:.*|start_at: \"$AT\"|' /opt/netmark/profile.yaml \
               && /opt/netmark/netmark /opt/netmark/profile.yaml" &
done
wait
```

Each process exits 0 on pass and 1 on fail, so the shell's exit statuses are your
result set.

### Two ways to get the profile there

**Preloaded.** Ship the profile with the binary — in the image, the package, the
ConfigMap — and have each machine run `netmark /opt/netmark/profile.yaml`. Nothing
listens on the network and there is nothing to secure. Best for a fixed fleet and
for CI, where the profile is version-controlled next to the code.

**Pushed by a controller.** Turn on the REST API on every machine and have one
controller send each of them a profile and collect the reports:

```sh
# on every machine, once
netmark
> restapi enable 0.0.0.0:8081
> configure save
```

```python
# on the controller
import concurrent.futures as cf, datetime, json, requests

start_at = (datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

def run(host, profile):
    profile = dict(profile, start_at=start_at)
    return requests.post(f"http://{host}:8081/api/v1/runs", json=profile).json()

with cf.ThreadPoolExecutor(max_workers=16) as pool:
    jobs = {pool.submit(run, "10.0.0.7", server_profile): "receiver"}
    for n in range(1, 11):
        jobs[pool.submit(run, f"10.0.0.{10+n}", client_profile)] = f"sender-{n}"
    for job in cf.as_completed(jobs):
        report = job.result()
        print(jobs[job], report["result"],
              report["sent_bytes_per_second"], report["received_bytes_per_second"])
```

`POST /api/v1/runs` blocks until the run and its debrief have finished and then
returns the report, so fan the requests out in parallel and let `start_at` do the
synchronising rather than trying to time the HTTP calls themselves.

The REST API is unauthenticated and can start traffic runs. Moving it off
loopback, as `restapi enable 0.0.0.0:8081` does, is only safe on a closed test
network or behind a proxy that authenticates. On anything else, keep it on
loopback and reach it over an SSH tunnel, or use preloaded profiles instead.

### Collecting the results

Each machine keeps its own `log/netmark.sqlite`, and every row says which role
wrote it, so the databases can be copied to one place and read together:

```sh
for host in receiver sender-{1..10}; do
  scp "$host":/opt/netmark/log/netmark.sqlite "results/$host.sqlite"
done
```

Or point them all at one external database, which gives you a single
`netmark_metrics` table with one row per run per machine:

```
> configure metrics postgresql://netmark:secret@10.0.0.2:5432/netmark
> metrics enable
> configure save
```

Run ids are per machine, so join on the timestamp rather than on the id.

---

## Bandwidth reporting

Every run reports upload and download bandwidth, everywhere:

- **stdout**, once per second while running and once at the end:
  `run 255 result=ok sent_bytes=51200 received_bytes=51200 up=17066 bytes/sec down=17066 bytes/sec`
- **`log/netmark.log`**, the same line with a timestamp.
- **Local SQLite**, in the `sent_bytes_per_second` and `received_bytes_per_second`
  columns of `runs`, and shown by `list` and `show run <id>`.
- **External SQL**, in the same two columns of `netmark_metrics`.
- **REST API**, as `sent_bytes_per_second` and `received_bytes_per_second`.
- **SDK**, on `RunReport`.

For TCP runs, the final stdout/log line, REST response, SDK report and `status`
table also show the negotiated MSS, path MTU and effective socket send window.
Set both TCP socket buffers with `tcp_window_size` (or `configure tcp window`);
the value `0` keeps the operating-system default.

Bandwidth is bytes moved divided by the wall-clock length of the run, floored at
one second so a very short run cannot report an inflated figure.

---

## The debrief

When the traffic stops, the client connects to the server on TCP port 9001 and
they reconcile:

```
debrief run=255 role=client protocol=udp sent_packets=50 received_packets=50 sent_bytes=51200 received_bytes=51200 lost=0 out_of_order=0 result=match
```

Packets and bytes must correlate exactly, with no loss and no reordering, or the
run fails with the reason recorded. Both sides write their own copy to their own
netmark.log and to the `debriefs` table in their own database, so a database
pulled off either host tells that host's side of the story.

Counters are kept per instance rather than per client, so one debrief covers
every client an instance drove; it is sent to the lowest-numbered client's remote.

---

## Where data is stored

### Logs

In `log/` next to the binary:

| File | Contents |
| --- | --- |
| `netmark.log` | Run lifecycle, bandwidth, debriefs, jitter alarms |
| `client.log` | Every send, tagged `client=<id>` |
| `server.log` | Every receive |
| `cli.log` | Every command typed |
| `monitor.log` | Monitor checks |
| `alarm.log` | Monitor failures |

### Local SQLite

`log/netmark.sqlite`, always on. Four tables — `runs`, `debriefs`, `alarms` and
`monitor_status` — and **every one of them has a `role` column** saying which side wrote the row:
`client`, `server`, `client+server` or `none`. A database copied off any host
therefore says plainly what that host was doing.

```sql
SELECT id, role, result, sent_bytes_per_second, received_bytes_per_second FROM runs;
SELECT run_id, role, matched, mismatch_reason FROM debriefs;
```

Millisecond-level metrics are never stored here; they only go to external SQL.
The same is true of monitoring: SQLite keeps only `monitor_status`, one row for
each time the monitor was started or stopped, while every check itself is stored
in the external database.

### External SQL

**Off by default.** Nothing leaves the host until you set a connection string,
either in netmark.config or with `configure metrics <connection>`, and then
`metrics enable`. PostgreSQL and SQLite are both supported. Each run writes
exactly one row to `netmark_metrics`.

`init.sh` brings up a local PostgreSQL in kind and writes the connection string
for you, if you want one.

---

## Monitoring and alerts

`monitor IP <url>` and `monitor start` fetch a URL every 30 seconds, counting
successes and failures and logging every failure to `alarm.log`.

**All monitor data is stored in the external (PostgreSQL) database**, one row per
check in `netmark_monitor` with the timestamp, monitor id, call id, target,
result, latency in milliseconds and the failure detail. The local SQLite database
holds only the start and stop status, in `monitor_status`. Checks made while no
external database is connected are logged but not stored.

```sql
SELECT timestamp_utc, target, result, latency_millis FROM netmark_monitor ORDER BY timestamp_utc;
```

SMTP is configured with `configure smtp <host[:port]>` and turned on with
`admin smtp enabled`, which first opens a connection and completes an EHLO
exchange so a broken server is caught immediately rather than at the moment an
alert is needed. `admin smtp status` re-runs that check on demand.

---

## The REST API

`restapi enable` serves the SDK over HTTP. The full contract is in
[openapi.yaml](openapi.yaml), and the running service serves that same document
from `/api/v1/openapi.yaml`.

```
GET  /                        the web interface with the web CLI
GET  /api/v1/health
GET  /api/v1/openapi.yaml
GET  /api/v1/profile          a default profile to use as a template
GET  /api/v1/clients
GET  /api/v1/runs
GET  /api/v1/runs/{id}
POST /api/v1/runs             body: a test profile (JSON or YAML); runs it and returns the report
POST /api/v1/cli              body: {"command": "..."}; runs a web CLI command
```

```sh
curl -s localhost:8081/api/v1/profile > profile.json
curl -s -X POST localhost:8081/api/v1/runs -H 'content-type: application/json' -d @profile.json
```

It defaults to `127.0.0.1:8081`. **The API is unauthenticated and can start
traffic runs**, so it stays on loopback unless you deliberately move it with
`restapi enable <address>`, and it should only ever be exposed behind something
that authenticates.

---

## The web interface and the web CLI

`GET /` on the REST API address serves a self-contained page with a **web CLI**:
a browser terminal over `POST /api/v1/cli`. It runs the same command core as the
shell CLI (`src/session.rs`), so every command in the reference above behaves the
same way and prints byte-identical output in both places, and its prompt shows
the same roles. Runs can also be started by posting a profile — JSON or an
unchanged YAML file from `profiles/` — to `/api/v1/runs`.

Above the terminal the page shows a **status section**: what is running right
now, the transport in use, whether the kernel supports SCTP, the current run and
its elapsed time, bytes and bandwidth in both directions, the server and client
state and the web server address. It polls `GET /api/v1/status` once a second,
so it follows a run live and is never reloaded.

The same commands work from bash/shell through the `netmarkctl` script in the
repository root, so a netmark service can be controlled both from the browser
and locally from a terminal:

```sh
./netmarkctl status
./netmarkctl list
./netmarkctl show 215
./netmarkctl run profiles/udp-10kbps.yaml
NETMARK_URL=http://other-host:8080 ./netmarkctl status   # point it elsewhere
```

For containers and services there is a headless mode that skips the interactive
terminal and only serves the web interface and the REST API:

```sh
netmark --serve                 # the configured (loopback) REST API address
netmark --serve 0.0.0.0:8080    # explicit address, e.g. inside a container
```

---

## Running in Kubernetes

`init.sh` brings up a complete local deployment. With [kind](https://kind.sigs.k8s.io)
installed it creates a cluster from `k8s/kind-config.yaml` with three nodes:

- a control plane,
- a **database node** (label `netmark.io/role: database`) running
  `k8s/postgres.yaml`,
- an **app node** (label `netmark.io/role: app`) running the netmark
  application (`k8s/netmark.yaml`, image built from `k8s/Dockerfile.netmark`)
  in `--serve` mode, wired to the database service.

The cluster is kept to three containers:

| Container | Where | What it does |
| --- | --- | --- |
| `postgres` | `k8s/postgres.yaml` | PostgreSQL with Timescale; holds all run metrics and all monitor data |
| `volume` | `k8s/postgres.yaml` | Owns the persistent volume claim, checks it is writable and reports how full it is |
| `netmark` | `k8s/netmark.yaml` | The web server: the web CLI and the REST API on port 8080 |

Both deployments prefer their labeled node but still schedule on single-node
clusters such as Docker Desktop. The app node's NodePort 30080 is mapped to the
host, so after `init.sh` finishes:

- the web interface and web CLI are at <http://127.0.0.1:8080>,
- `./netmarkctl <command>` controls the same service from the shell,
- PostgreSQL is port-forwarded to `127.0.0.1:5433` and the connection string is
  written into `netmark.config` for a host-side netmark.

`k8s/cluster-test.sh` checks that the cluster is up and healthy. It runs one
case per expectation — the API is reachable, the namespace exists, the
`postgres` and `volume` containers are deployed, both deployments have a ready
replica, the data volume is bound, PostgreSQL accepts connections, the volume
container owns the data volume, and the web server answers both `/api/v1/status`
and the web CLI — printing `ok` or `FAIL` for each and exiting non-zero if any
case fails. Set `NETMARK_NAMESPACE` or `NETMARK_WEB` to test another deployment.

---

## Using netmark as a Rust library

netmark is a library as well as a binary.

```rust
use netmark::sdk::TestRunner;

let report = TestRunner::from_profile_file("profiles/udp-10kbps.yaml".as_ref())?
    .run()?;

assert!(report.passed);
println!("up {} B/s, down {} B/s",
    report.sent_bytes_per_second,
    report.received_bytes_per_second);
```

### Hooks

A profile can name Rust callbacks in `hooks.before` and `hooks.after`; you
register them on the runner by name. An `after` hook receives the report and
fails the run by returning an error. This is how you attach assertions that
netmark itself knows nothing about.

```rust
let report = TestRunner::from_profile_file("profiles/udp-hooks.yaml".as_ref())?
    .hook("announce", |context| {
        println!("starting run {}", context.run_id);
        Ok(())
    })
    .hook("require_traffic", |context| {
        match context.report {
            Some(report) if report.sent_bytes() == 0 => Err("no traffic".into()),
            _ => Ok(()),
        }
    })
    .run()?;
```

A profile that names a hook nobody registered is rejected, rather than silently
skipping the code it asked for. See `examples/sdk_hooks.rs`.

---

## Ports and permissions

| Port | Protocol | Used for |
| --- | --- | --- |
| 9000 | TCP and UDP | Traffic |
| 9001 | TCP | End-of-run debrief |
| 8080 | TCP | Built-in test HTTP server |
| 8081 | TCP | REST API, loopback by default |
| — | IP protocol 253 | Raw IP traffic; needs `CAP_NET_RAW` |
