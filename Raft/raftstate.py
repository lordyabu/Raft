from raftobjects import AppendEntriesResponse, AppendEntriesCommand, LogEntry, SendRequestVoteCommand, SendRequestVoteResponse, LeaderSend
import pandas as pd
import pickle
import numpy as np
import time
import math
from raftnet import RaftNet
import random
from appendentries import append_entries
import raftconfig






# Handles state of Raft server. This does not handle client/server requests, but is meant as a logical tracking system for
# server states. It has capabilities of storing both leader/follower states as needed. In practical Raft
# There around 20 million states to account for making it very difficult to track follower states for edge case
# state transitions. Currently, this implementation of Raft doesn't contain many edge cases, but handles the core issues
# surrounding Raft such as leader elections, loss in leader/follower servers, rebooting of servers, saving logs to local
# storage, appending entries correctly, and sending messages between servers.
class RaftState:
    def __init__(self, nodenum: int, position: int):


        # For debugging
        self.print_stuffs = False


        # Leader elections
        # ----------------------
        # Stores current leader term -> vote as a dictionary
        self.term_to_vote = {}
        # Current votes being received. Does not update monotonically and is only added on election request
        self.votes_received = 0
        # ----------------------


        # Server life
        # ----------------------

        # If sever should be alive. This is current implementation for difficult edge cases, and is updated
        # if an unfavorable state is reached
        self.should_be_alive = 1

        # Hard code for TESTING. Server initializes to (0 : Follower) or (1 : Leader)
        if position == 1:
            self.role = "Leader"
        else:
            self.role = "Follower"

        # Hard code for TESTING  Server initializes to-> (index in raftconfig.SERVER) which is the leader
        self.leader_num = 0
        # Node number for this follower -> raftconfig.SERVERS
        self.nodenum = nodenum
        # Above nodes KV value in raftconfig.SERVERS
        self.server_address = raftconfig.SERVERS[nodenum]
        # ----------------------

        # Raft Log Replication
        # ----------------------
        # Stores last time since last sent/seen heartbeat.
        self.last_heartbeat_time = time.time()
        # Stores position in log. ToDo: Make this variable current_index
        self.position = position
        # Current Log. Note for c++ implementation will need to store this variable : probably will use a doubly linked list
        self.log = np.array([])
        # Servers current LogEntry term
        self.current_term = 1
        # Servers previous LogEntry term
        self.prev_term = -1
        # Index the server is on. Note not the position variable
        self.match_index = 0
        # Prev index of log
        self.prev_index = -1  # -1 because it means it will add the next entry at index 0
        # ----------------------

        # Keep track of other Servers next_index, term, and whether it's alive
        # ----------------------
        self.follower_states = {}  # Dictionary to store follower states
        for follower_index in raftconfig.SERVERS:
            if follower_index != nodenum:
                self.follower_states[follower_index] = {
                    "next_index": 0,
                    "prev_term": -1,
                    "curr_term": -1,
                    'is_alive': .5  # Initialize current term for each follower
                    # Add other follower state variables as needed
                }
        # ----------------------

        # System for communicating between servers. Note in c++ implementation this will need big changes because of
        # sockets and threading
        self.raft_net = RaftNet(self.nodenum, self.position)

        # Not ideal implementation. This in the future should be stored in raftnet.py.
        # This stores address of client connections
        self.client_socket_addrs = []


    # Saves logs locally using pandas df -> csv
    def local_storage_log(self, log, nodenum):
        df = pd.DataFrame(columns=['command'])
        df['command'] = log
        df.to_csv(f'./Data/Log{nodenum}.csv')

    # use pickle or parquet. This is a difficult problem. Not covered in Raft paper much
    def persistent_storage_log(self, df):
        pass

    # This function is ONLY called upon by LEADER_SERVER to append their new log entries

    # ToDo: add edge cases for fail state, but low priority because this a pretty fail safe method
    def handle_client_log_append(self, msg):
        # Client adds a log entry (received by leader)
        new_entry = LogEntry(self.current_term, msg)  # Create a new log entry
        self.log = np.append(self.log, new_entry)  # Add the new entry to the leader's log
        self.prev_index = len(self.log) - 1 # Update prev_index
        self.prev_term = self.current_term # Sets prev_term to current_term
        self.current_term = self.log[-1].term # Increments new term. Note c++ this will need to be different
        self.local_storage_log(self.log, self.nodenum) # Calls upon local storage method
        self.match_index = len(self.log) # Updates match_index of state
        return True

    # This function is ONLY called upon by FOLLOWER_SERVER(S) to append their new log entries and RETURN a response

    # Uses an AEC object from leader and SENDS AER object to leader
    def handle_append_entries(self, command_and_details):

        # DEBUGGING
        if self.print_stuffs:
            print(command_and_details)

        # Update to the log (received by a follower) and other state changes
        self.reset_heartbeat_timer() # First reset time since seen heartbeat
        entries = command_and_details.entries # Getting vars ---
        prev_index = command_and_details.prev_index #        ---
        prev_term = command_and_details.prev_term #          ---
        reset_server = command_and_details.reset #           ---

        # Use append_entries() to add new entries to the follower's log
        append_entries_response, self.log = append_entries(self.log, prev_index, prev_term, entries, self.nodenum,
                                                           reset_server)

        append_entries_response = AppendEntriesResponse(append_entries_response[0], append_entries_response[1], append_entries_response[2], append_entries_response[3], append_entries_response[4])



        # Handles main two cases for append entries response
        if not append_entries_response.success:

            # If server's log entry and AER is success false
            if prev_index == -1:
                # DEBUGGING
                if self.print_stuffs:
                    print(f"SEVER {self.nodenum} IS DEAD")

                #ToDo: Add actual command here
            else:
                # Currently using catchup non recursive function ToDo: make recursive
                self.raft_net.send(self.leader_num, pickle.dumps(append_entries_response))
        else:

            # If AER success true
            self.prev_index = len(self.log) - 1 # Updating vars ---
            self.prev_term = self.current_term #                ---
            self.current_term = self.log[-1] #                  ---
            self.local_storage_log(self.log, self.nodenum) # Saving to local storage
            return append_entries_response # Returning AER to be handled by leader server


    # Send response object to leader server
    def handle_append_entries_response(self, msg):
        # Follower response (received by leader)
        pickled_response = pickle.dumps(msg)
        self.raft_net.send(self.leader_num, pickled_response)


    # Update follower states based on data received from AER object
    def update_follower_state(self, follower_index, next_index, prev_term, curr_term, alive):
        self.follower_states[follower_index]["next_index"] = next_index
        self.follower_states[follower_index]["prev_term"] = prev_term
        self.follower_states[follower_index]["curr_term"] = curr_term
        self.follower_states[follower_index]["is_alive"] = 1 if alive else 0


    # One of the most important functions. Is called upon by Leader server to append new commands to LogEntry
    # and send append entries command to follower servers. Then waits until there is consensus and sends consensu
    # result to client
    def handle_leader_heartbeat(self, addr, msg):


        # Update state vars
        self.reset_heartbeat_timer()
        prev_index = self.prev_index
        prev_term = self.log[prev_index].term if prev_index >= 0 else -1

        # Append new entries to LogEntry
        self.handle_client_log_append(msg)


        # Send AEC to all followers
        for follower_index in self.follower_states:
            if follower_index != self.nodenum:
                # Generate AEC object
                append_entries_command = AppendEntriesCommand(prev_index, prev_term, LogEntry(self.current_term, msg),
                                                              False)
                try:
                    # Pickle message and send
                    aec_pickle = pickle.dumps(append_entries_command)
                    self.raft_net.send(follower_index, aec_pickle)
                except:
                    # For DEBUGGING
                    if self.print_stuffs:
                        print(f"Follower {follower_index} is not working")


        # Wait for concensuss
        consensus = False
        while not consensus:
            consensus = self.has_consensus_happened()

        try:
            self.raft_net.send_client(addr, "ConsensusReached")
        except:
            pass
            # ToDo: handle this edge case properly as described in Raft paper


    # Current implementation of concensus. Note correct implementation below, but this is current implementation
    def has_consensus_happened(self):

        # Keep track of servers who are against concensus
        bad = 0

        # Iterate through servers
        for follower_index in raftconfig.SERVERS:
            if self.nodenum != follower_index:

                # Conditionals as stated in paper numerized
                if self.follower_states[follower_index]['is_alive'] == 1:
                    if self.follower_states[follower_index]['next_index'] != self.match_index:
                        bad += 1
                else:
                    pass

        # For when full 5 servers
        if bad > math.floor(len(raftconfig.SERVERS) / 2):
            return False

        return True


    # Mathmatical interpretation of concensus. Will replace above function in future versions
    def has_consensus_happened_ready(self):
            next_indexes = []
            for follower_index in raftconfig.SERVERS:
                next_indexes.append(self.follower_states[follower_index]['next_index'])

            if np.median(next_indexes) > self.match_index:
                return False

            return True


   # Command center for RaftState. Handles received messages from clients and other servers
    def handle_new_state(self, addr, msg):

        # IF message is SRVC object, generate SRVR object and send to candidate server
        if isinstance(msg, SendRequestVoteCommand):
            candidate_id = msg.candidate_id
            term, vote = self.handle_request_vote(msg)
            srvr = SendRequestVoteResponse(term, vote)
            srvr_pickle = pickle.dumps(srvr)
            self.raft_net.send(candidate_id, srvr_pickle)
        # If SRVR object, handle it
        elif isinstance(msg, SendRequestVoteResponse) and self.role == "Candidate":
            self.handle_request_vote_response(msg)
        # This should be an error. ToDo: Implement edge cse
        elif isinstance(msg, SendRequestVoteResponse) and self.role != "Candidate":
            pass
        # If LS object, handle it
        elif isinstance(msg, LeaderSend):
            self.role = 'Follower'
            self.leader_num = msg.leader_num
            self.reset_heartbeat_timer()

        # AEC, AER, COMMAND objects
        else:
            # If server is leader
            if self.role == "Leader":

                # If AER object a success, handle
                if isinstance(msg, AppendEntriesResponse) and msg.success:
                    self.update_follower_state(msg.follower_index, msg.next_index, msg.prev_term, msg.term, msg.success)
                # If AER object a fail, handle
                elif isinstance(msg, AppendEntriesResponse) and not msg.success:
                    append_entries_command = AppendEntriesCommand(-1, -1, self.log, True)
                    aec_pickle = pickle.dumps(append_entries_command)
                    self.raft_net.send(msg.follower_index, aec_pickle)
                else:
                    # Sends Command object to handle_leader_hearbeat()
                    self.handle_leader_heartbeat(addr, msg)
            # Not a leader and it's a AEC object, handle
            elif isinstance(msg, AppendEntriesCommand):
                aer = self.handle_append_entries(msg)
                self.handle_append_entries_response(aer)
            # Currently this logic leaves out "SwitchToLeader" string object which is sent back to client
            else:
                self.raft_net.sender_client(addr, "SwitchToLeader")

    # Timeout used for leader election
    def random_timeout(self):
        # Return a random timeout value between a certain range
        # ToDo: make timeout shorter
        return random.uniform(5, 10)

    # Reset time since lastheartbeat
    def reset_heartbeat_timer(self):
        self.last_heartbeat_time = time.time()

    # Handle election timeout. Only if significant if follower or candidate
    def handle_election_timeout(self):
        random_timeout = self.random_timeout()
        while True:
            # If follower and timeout become candidate
            if self.role == 'Follower' and time.time() - self.last_heartbeat_time > random_timeout:
                self.become_candidate()

            # If candidate and timeout call for an election and use a SRVC object
            if self.role == 'Candidate' and time.time() - self.last_heartbeat_time > random_timeout:
                self.reset_heartbeat_timer()
                random_timeout = self.random_timeout()
                if isinstance(self.current_term, LogEntry):
                    self.current_term = self.current_term.term
                self.current_term += 1
                self.votes_received = 1  # Vote for self
                self.send_request_vote_messages(SendRequestVoteCommand(self.current_term, self.prev_term, self.nodenum, self.prev_index))


    # Sends SRVC object through RaftNet to other servers

    def send_request_vote_messages(self, request_vote_command):
        # Iterate through server
        for index in raftconfig.SERVERS:
            if index != request_vote_command.candidate_id:

                # Try sending pickled SRVC object
                try:
                    rvc_pickle = pickle.dumps(request_vote_command)
                    self.raft_net.send(index, rvc_pickle)
                except:
                    # Create artificial SRVC object that is set to False
                    rvr_pickle = pickle.dumps(SendRequestVoteResponse(self.current_term, 0))
                    self.raft_net.send(request_vote_command.candidate_id, rvr_pickle)

                    # DEBUGGING
                    if self.print_stuffs:
                        print(f"Follower {index} is not working")


    # handle SRVC object as any server that isn't the candidate server
    def handle_request_vote(self, request_vote_command):
        # If leader, step down
        if self.role == "Leader":
            self.become_follower()


        # Bad SRVC object, so reject vote

        # Hack here ToDo: figure out why current_term becoming LogEntry, but contains correct current term
        if isinstance(self.current_term, LogEntry):
            self.current_term = self.current_term.term


        if request_vote_command.term < self.current_term:
            return self.current_term, 0

        #
        # if SRVC object term is higher than current term, update prev/current term, become follower
        elif request_vote_command.term > self.current_term:
            self.current_term = request_vote_command.term
            self.become_follower()
            self.prev_term = self.current_term
            self.current_term = request_vote_command.term


            # DEBUGGING
            if self.print_stuffs:
                print(self.current_term)


        # If the candidate's log is at least as up-to-date as ours, grant the vote
        self.term_to_vote[request_vote_command.term] = request_vote_command.candidate_id  # Remember who we voted for in this term
        self.reset_heartbeat_timer() # Reset time since last heard
        return self.current_term, 1

    # Handle SRVR object
    def handle_request_vote_response(self, send_request_vote_resposnse):
        # DEBUGGING
        if self.print_stuffs:
            print(send_request_vote_resposnse)

        # If the term of SRVR and current term matchup, become leader
        if send_request_vote_resposnse.term == self.current_term:
            if send_request_vote_resposnse.vote == 1:
                self.votes_received += 1
                if self.votes_received > len(raftconfig.SERVERS) / 2:
                    self.become_leader()

    # Become leader
    def become_leader(self):
        self.role = 'Leader'
        print("IM LEADER ON TERM", self.current_term)

        # Send LS object to all other servers
        force_follow = LeaderSend(self.leader_num)
        for index in raftconfig.SERVERS:
            if index != self.nodenum:
                try:
                    ff_pickle = pickle.dumps(force_follow)
                    self.raft_net.send(index, ff_pickle)
                except:
                    if self.print_stuffs:
                        print(f"Follower {index} is not working")

        # Append no-op into log

    # Become candidate
    def become_candidate(self):
        self.role = 'Candidate'


    # Become follower
    def become_follower(self):
        self.role = 'Follower'

