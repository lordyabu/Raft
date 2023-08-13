from socket import socket, AF_INET, SOCK_STREAM
from message import send_message, recv_message
import raftconfig
import pickle
from time import time

# Stores socket_to_add index to socket
socket_to_add = {}

# Create socket if not already added at index - raftconfig.SERVERS
def send_to_new_index(index):
    if index == 5:
        index = 0
    if index not in socket_to_add:
        addr = raftconfig.SERVERS[index]
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        socket_to_add[index] = sock
    else:
        sock = socket_to_add[index]
    return sock



# Run client at connection address addr
def main(addr):
    index = 0 # raftconfig.SERVERS
    # Try connecting to index 0
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(addr)
        socket_to_add[index] = sock
        # If successful process and receive messages
        while True:
            msg = input("Say >")
            if not msg:
                break
            send_message(sock, pickle.dumps(msg))
            response = recv_message(sock)
            print(pickle.loads(response))


            # If response is SwitchToLeader try connecting to different server
            if pickle.loads(response) == "SwitchToLeader":
                print("Switching to the leader...")
                max_attempts = 5  # Maximum number of connection attempts
                index += 1
                for _ in range(max_attempts):

                    # THIS PRINT IS CURRENTLY NEEDED DON'T DELETE
                    print(index)
                    # Try to connecting to new socket and sending msg
                    try:
                        sock = send_to_new_index(index)
                        send_message(sock, pickle.dumps(msg))
                        response = recv_message(sock)
                        print(pickle.loads(response))
                        if pickle.loads(response) == "ConsensusReached":
                            break  # Exit the loop if consensus is reached
                        index = (index + 1) % len(raftconfig.SERVERS)
                    # If fail increment socket index by 1
                    except:
                        index = (index + 1) % len(raftconfig.SERVERS)

        # Close socket
        sock.close()
    except:
        # Recursively call main on different index
        index = (index + 1) % len(raftconfig.SERVERS)
        main(raftconfig.SERVERS[index])


# Base command
main(('localhost', 1000))
