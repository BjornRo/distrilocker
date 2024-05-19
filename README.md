# distrilocker
Distributed locking of values, for example registration and locking a username until db-query. Just needed for my own project until subinterpreters exist.

Server is simply started by using arguments to the file.
Either select tcp://address:port or unix/unix://path/my/no, then number of stores.
python server.py tcp://0.0.0.0:10101 7

METHOD := get | set | update | delete | size

To request from the client. The client send: Request as: Len(Request)||Request||Data
Server responds with: OK/ERR[1byte]||ResultAsBytes

The server handles all requests using a dict and each store has their own dict and lock. The server multiplexes
using the STORE_ID given by the client. 

Using periodic: Each store also have a cleaning task that selects some random keys and checks the expiry, if it is expired, delete.
Otherwise, task deletes itself. I dont know which version is the best...


Only stable enough for localhost usage