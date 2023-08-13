from raftobjects import  LogEntry, AppendEntriesResponse
import numpy as np


# Logic implemented below of AppendEntries RPC function, but uses 0 based indexing. Note not exact logic but includes main
def append_entries(log, prev_index, prev_term, entries, follower_num, reset):

    # IF term higher than current term, make follower, if it is less, you can either reply no and send current term, or just return a mistake
    # For broken log
    if reset:
        log = np.array([])

    # Implementation given np array. Note c++ implementation will be different
    if not isinstance(entries, list):
        entries = [entries]

    print(entries)


    # If new server/log, update log and return success AER object
    if prev_index == -1:
        log = np.append(log, entries)
        return (True, prev_term, entries[-1].term, follower_num, len(log)), log

    # If prev_index is higher than current length of log, return false AER object. ToDo: note use len(log) and send variable instead
    if prev_index >= len(log):
        if len(log) > 0:
            return (False, None, log[-1].term, follower_num, len(log)), log
        else:
            return (False, None, -1, follower_num, len(log)), log


    # If should be second entry in log and previous terms don't match up, return false AER object
    if prev_index == 0:
        if len(log) > 0 and log[0].term != prev_term:
            return (False, None, log[-1].term, follower_num, len(log)), log
    # If previous terms don't match up. Note this is different in Raft paper but is needed because of 0 based indexing
    elif log[prev_index].term != prev_term:
        return(False, None, log[-1].term, follower_num, len(log)), log

    # If log is bigger than the previous index, but the entry is saying it's term is higher than log term, the current log should truncate, add new response
    # and return True
    if len(log) >= prev_index + 1:
        if log[prev_index].term < entries[0].term:
            log = np.delete(log, np.s_[prev_index:], axis=0)
            log = np.append(log, entries)
            return (True, prev_term, entries[-1].term, follower_num, len(log)), log


    # Otherwise success, add entries and return true
    log = np.append(log, entries)
    return (True, prev_term, entries[-1].term, follower_num, len(log)), log

6
# Test usage
if __name__ == '__main__':
    log = [LogEntry(1, "A"), LogEntry(1, "B"), LogEntry(2, "C"), LogEntry(2, "X"), LogEntry(2, "Y")]
    # log = [LogEntry(1, "A"), LogEntry(1, "B")]
    prev_index = 1
    prev_term = 1
    entries = [LogEntry(3, "D")]
    result = append_entries(log, prev_index, prev_term, entries, 1)
    print(result)  # Output: True
    for entry in log:
        print(entry)
