# netmark
CLI load testing tool

The same binary for client and server, to create traffic between two computers just start same binary on both servers and  use CLI commands "server enable" on one and "enable client" on the other. You can also run both server and client on the same computer just to test things out.

It also has a monitor function that will check that a configurable address on the internet is accessible and log alarms when it's not reachhing that point.

Plans:
Send email when monitor fails.
Add traffic patterns and more protocols to the client.
Write data into a database for graphing.
Better status page to show what is going on in more detail.

How to use:
Just run the binary netmark and you'll get a prompt, type help and go from there.

SMTP:
Set the server with `configure smtp <host[:port]>`, then turn it on with
`admin smtp enabled` or off with `admin smtp disabled`. Enabling first verifies
the server answers with an SMTP greeting; `admin smtp status` re-runs that check
at any time.

Clients:
One instance can drive several clients at once. Each has its own id, starting at
0, its own destination and optional runtime and jitter overrides, and every
command that configures or controls a client takes that id:
`client list`, `client add`, `client delete <id>`, `client <id> enable`,
`client <id> disable`, `client <id> remote <ip>`, `client <id> runtime <seconds>`,
`client <id> jitter <ms>`, `client <id> http check <url>`, `client <id> status`.
UDP sequence numbers are namespaced per client id, so several clients sending to
one server are not mistaken for reordering.

Debrief:
Every run ends with a debrief between client and server over TCP port 9001. The
client reports what it put on the wire, the server reports what it took off, and
each side writes the reconciliation to its own netmark.log and local SQLite
`debriefs` table. Packets and bytes must correlate exactly, with no loss and no
reordering, or the run fails. `show run <id>` prints the stored debriefs.

Roles in the local database:
Every table in the local SQLite database carries a `role` column saying which
side wrote the row: `client`, `server`, `client+server` or `none`. It is also
printed by `list` and `show run <id>`, so a database copied off any host says
plainly what that host was doing.

REST API:
`restapi enable [<address>]` serves the SDK over HTTP; `restapi disable` stops it
and `restapi status` shows where it is listening. It defaults to
`127.0.0.1:8081`, because the API is unauthenticated and can start traffic runs.
The contract is in [doc/openapi.yaml](doc/openapi.yaml) and the running service
serves that same document from `/api/v1/openapi.yaml`.

As a library:
netmark is also a Rust library. `netmark::sdk::TestRunner` runs a test profile
from code and returns a `RunReport` you can assert on. A profile may name Rust
callbacks in its `hooks.before` and `hooks.after` lists; register them on the
runner with `.hook("name", ...)`. An `after` hook that returns an error fails the
run. See `examples/sdk_hooks.rs` and `profiles/udp-hooks.yaml`.

Better documentation to come.
