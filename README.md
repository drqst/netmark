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

As a library:
netmark is also a Rust library. `netmark::sdk::TestRunner` runs a test profile
from code and returns a `RunReport` you can assert on. A profile may name Rust
callbacks in its `hooks.before` and `hooks.after` lists; register them on the
runner with `.hook("name", ...)`. An `after` hook that returns an error fails the
run. See `examples/sdk_hooks.rs` and `profiles/udp-hooks.yaml`.

Better documentation to come.
