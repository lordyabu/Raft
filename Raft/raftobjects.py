# These are the variables that are sent between sockets. This implementation uses pickle                               v
class LogEntry:
    def __init__(self, term, command):
        # Current leader term
        self.term = term
        # Message to be stored in log
        self.command = command

    def __str__(self):
        return f"LogEntry(term={self.term}, command='{self.command}')"


    def to_string(self):
        return f"{self.term} {self.command}"


# AEC.

# This is an object that 1. contains the above LogEntry and 2. Is sent from the leader -> follower server to update logs

class AppendEntriesCommand:
    def __init__(self, prev_index, prev_term, commands, reset):
        # Commands to store in logs(LogEntry object)
        self.entries = commands

        # Correct previous index of leader log
        self.prev_index = prev_index

        # Correct previous index of leader log
        self.prev_term = prev_term

        # Whether this is a reset log. Note future implementation should switch this into recursive function as noted in Raft paper
        self.reset = reset


    # Ignore this
    def to_string(self):
        return f"{self.entries} {self.prev_index} {self.prev_term} {self.reset}"

    def __str__(self):
        return f"Entries{self.entries}, Prev_Index={self.prev_index}, Prev_Term={self.prev_term}, Reset={self.reset}"


# AER

# This is an object that is sent back from follower server to leader server regarding their attempt of AppendEntries
class AppendEntriesResponse:
    def __init__(self, success, prev_term, term, follower_index, next_index):
        # True if log replication was successful, False otherwise
        self.success = success

        # Previous term of the follower
        self.prev_term = prev_term

        # Current term of the follower
        self.term = term

        # Index of the follower that sent the response. Is a key of raftconfig.SERVERS
        self.follower_index = follower_index

        # Log Index up to which follower has replicated
        self.next_index = next_index



# This is sent when a follower server doesn't hear leader for X seconds. to change this variable look in raftstate.handle_election_timer()
class SendRequestVoteCommand:
    def __init__(self, term, prev_term, candidate_id, last_log_index):
        # Last term of candidate
        self.prev_term = prev_term

        # Current term the candidate's log entry is on
        self.term = term

        # Index of the candidate that sends the election call. Is a key of raftconfig.SERVERS
        self.candidate_id = candidate_id

        # Last log index of candidate
        self.last_log_index = last_log_index



# This is an object that is sent back from a follower to a candidate server regarding their vote in the election
class SendRequestVoteResponse:
    def __init__(self, term, vote):
        # Term of follower
        self.term = term

        # Vote. 1 in True else 0
        self.vote = vote


# Object sent when candidate becomes leader to tell other servers to become followers and not run an election
class LeaderSend:
    def __init__(self, leader_num):
        # Index who which is now the leader. Is a key of raftconfig.SERVERS
        self.leader_num = leader_num