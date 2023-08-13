import threading
import socket
from queue import Queue
import raftconfig
import pickle
from raftstate import RaftState


# RaftServer
class RaftServer:
    def __init__(self, nodenum: int, position: int):
        self.raft_state = RaftState(nodenum, position)

        self.print_stuffs = False


# Example usage of RaftServer object. Note this will be very different for c++ implementation
def console(nodenum, is_leader):
    net = RaftServer(nodenum, is_leader)
    message_queue = Queue()  # Create a queue to store received messages

    def receiver():
        while True:
            addr, msg = net.raft_state.raft_net.receive()
            if msg != b'' and msg != ' ':
                try:
                    unpickled_msg = pickle.loads(msg)
                    if unpickled_msg is not None:
                        print("Received message:", unpickled_msg, net.raft_state.role)
                        message_queue.put((addr, msg))
                except pickle.UnpicklingError as e:
                    if net.print_stuffs:
                        print("Error unpickling message:", e)

    def message_processor():
        while True:
            addr, msg = message_queue.get()  # Get a message from the queue
            msg = pickle.loads(msg)
            net.raft_state.handle_new_state(addr, msg)
            message_queue.task_done()  # Mark the message as processed

    threading.Thread(target=receiver).start()  # Start the receiver thread

    # Create and start multiple message processor threads
    num_processors = 3  # You can adjust the number of message processor threads
    for _ in range(num_processors):
        threading.Thread(target=message_processor).start()

    threading.Thread(target=net.raft_state.handle_election_timeout).start()


if __name__ == '__main__':
    import sys
    console(int(sys.argv[1]), int(sys.argv[2]))