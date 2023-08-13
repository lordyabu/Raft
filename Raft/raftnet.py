import threading
import socket
from queue import Queue
import raftconfig
from socket import AF_INET, SOCK_STREAM
import pickle
from message import recv_message, send_message



# Communication system between RaftServers. Uses sockets, queues, and threads. Note c++ implementation
# will be vastly different here. Taking advantage of python sockets and Queues
class RaftNet:
    def __init__(self, nodenum: int, position: int):

        # DEBUGGING
        self.print_stuffs = False

        # Server life
        # ----------------------
        self.should_be_alive = 1
        if position == 1:
            self.leader = True
        else:
            self.leader = False

        self.leader_num = 0
        self.first_send = True
        # ----------------------

        # Raft Communications Receiving
        # ----------------------
        self.nodenum = nodenum
        self.server_address = raftconfig.SERVERS[nodenum]
        self.receive_queue = Queue()
        self.socket = socket.socket(AF_INET, SOCK_STREAM)
        self.socket.bind(self.server_address)
        self.socket.listen()
        self.receive_sockets = [self.socket]  # Add the main server socket to the list

        # Start the receiver thread
        threading.Thread(target=self.receiver).start()
        # ----------------------

        # Sending
        # ----------------------
        self.client_sockets = {}
        # Setup queues for sending and receiving messages
        self.send_queues = {node: Queue() for node in raftconfig.SERVERS}
        for d in raftconfig.SERVERS:
            threading.Thread(target=self.send_responses_runner, args=[d]).start()
        self.other_offlines = []
        # ----------------------

    # Runs the send for server. waits until message is in queue, and send to appropriate socket

    # -----------------------------------------------------------------------------------
    # Puts message into sendqueue. Note: does not send the message, this is sent in sender
    def send(self, destination: int, message: bytes):
        # Put the message in the destination's send queue
        if self.print_stuffs:
            print("SENDING TO", destination)
        self.send_queues[destination].put(message)
    def send_responses_runner(self, destination):
        while True:
            try:
                message = self.send_queues[destination].get()
                if self.print_stuffs:
                    print(pickle.loads(message))
                client_socket = self.get_client_socket(destination)
                send_message(client_socket, message)
            except:
                self.other_offlines.append(destination)
                if self.print_stuffs:
                    print(f"Follower {destination} is not online")

    # --------------------------------------------------------------------------------------


    # Alterations of ABOVE send and sender methods for client use --------------------------------
    def send_client(self, addr, msg):
        threading.Thread(target=self.sender_client, args=[addr, msg]).start()

    def sender_client(self, addr, msg):
        send_message(addr, pickle.dumps(msg))

    # --------------------------------------------------------------------------------------


    # Note this is not client objects but way to create socket connections but servers
    def get_client_socket(self, destination: int):
        if destination not in self.client_sockets:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_address = raftconfig.SERVERS[destination]
            client_socket.connect(client_address)
            self.client_sockets[destination] = client_socket
        return self.client_sockets[destination]

    # Runs the receive for server. waits until message is in queue, and receives

    # -----------------------------------------------------------------------------------
    # Puts message into receive_queue
    def receive(self):
        return self.receive_queue.get()

    def receiver(self):
        client, addr = self.socket.accept()
        threading.Thread(target=self.receiver).start()
        while True:
            data = recv_message(client)
            self.receive_queue.put((client, data))
    # -----------------------------------------------------------------------------------


    # Remove connections from connection list
    def remove_connections_server(self, server_num):
        del self.client_sockets[raftconfig.SERVERS[server_num]]
