# distrilocker
Distributed locking of values, for example registration and locking a username until db-query. Just needed for my own project until subinterpreters exist.

Server is simply started by using arguments to the file.
Either select tcp://address:port or unix/unix://path/my/no, then number of stores.

Using ascii and some binary as main transfer protocol.

METHOD := get | set | update | delete

To request from the client. The client send: STORE_ID||METHOD||ARGS..||\n
The client then receives the result in the format of HEADER(2 bytes binary big-endian)||INTEGER
The header is to know how many integers needs to be read and also extension.
