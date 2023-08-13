# raftconfig.py

# Mapping of logical server numbers to network addresses.  All network
# operations in Raft will use the logical server numbers (e.g., send
# a message from node 2 to node 4).
SERVERS = {
    0: ('localhost', 1000),
    1: ('localhost', 1111),
    2: ('localhost', 1222),
    3: ('localhost', 1333),
    4: ('localhost', 1444),
    }