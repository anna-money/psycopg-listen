## v0.0.1

* A first version: a psycopg port of asyncpg-listen.
* Liveness follows the psycopg recommendation — no `SELECT 1` polling; `notifies()` waits on the socket and surfaces a dropped connection, TCP keepalives cover silent deaths.
