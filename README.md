# distrilocker
Distributed locking of values, for example registration and locking a username until db-query. Just needed for my own project until subinterpreters exist.

Server is simply started by using arguments to the file.
Either select tcp://address:port or unix/unix://path/my/no, then number of stores.
python server.py tcp://0.0.0.0:10101 7

Using ascii and some binary as main transfer protocol.

METHOD := get | set | update | delete

To request from the client. The client send: STORE_ID||METHOD||ARGS..||\n
The client then receives the result in the format of HEADER(2 bytes binary big-endian)||INTEGER
The header is to know how many integers needs to be read and also extension.


The server handles all requests using a dict and each store has their own dict and lock. The server multiplexes
using the STORE_ID given by the client. Each store also have a cleaning task that selects some random keys and checks the expiry, if it is expired, delete.