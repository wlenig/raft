import string
import argparse, socket, json, time, enum, random
from typing import TypedDict


BROADCAST_ID = 'FFFF'
# seconds range for election timeout
MIN_ELECTION_TIMEOUT = 0.3
MAX_ELECTION_TIMEOUT = 0.5
# seconds before min_election_timeout that heartbeats will be sent by leader
LEADER_PREEMPTIVITY = 0.1

# safety
assert(MIN_ELECTION_TIMEOUT - LEADER_PREEMPTIVITY > 0)


class ReplicaMode(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class LogEntry(TypedDict):
    # term when entry was received by leader
    term = int
    put_key = str
    put_value = str


class MessageType(str, enum.Enum):
    GET = 'get'
    PUT = 'put'
    HELLO = 'hello'
    FAIL = 'fail'
    OK = 'ok'
    REDIRECT = 'redirect'
    REQUEST_VOTE = 'request_vote'
    REQUEST_VOTE_RES = 'request_vote_res'
    APPEND_ENTRIES = 'append_entries'
    APPEND_ENTRIES_RES = 'append_entries_res'


class RequestVoteArgs(TypedDict):
    # candidate's term
    term: int
    # candidate requesting vote
    candidate_id: str
    # index of candidate's last log entry
    last_log_idx: int
    # term of candidate's last log entry
    last_log_term: int


class AppendEntriesArgs(TypedDict):
    # leader's term
    term: int
    # so follower can redirect clients
    leader_id: int
    # index of log entry immediately preceding new ones
    prev_log_idx: int
    # term of prevLogIndex entry
    prev_log_term: int
    # log entries to store (empty for heartbeat; may send more than one for efficiency)
    entries: list[LogEntry]
    # leader's commit index
    leader_commit_idx: int


class AppendEntriesResults(TypedDict):
    # current_term, for leader to update itself
    term: int
    # true if follower contained entry matching prevLogIndex and prevLogTerm

    success: bool


class RequestVoteResults(TypedDict):
    # currentTerm, for candidate to update itself
    term: int
    # true means candidate received vote
    vote_granted: bool


class Replica:
    def __init__(self, port: int, id: str, others: list[str]):
        self.port = port
        self.id = id
        self.others = others

        # timeout is used by both Leader and non-leaders
        # leaders use it to send heartbeats
        # non-leaders use it as an election timeout
        self.timeout = None

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('localhost', 0))
        self.socket.settimeout(0.05)
        self._send_hello()

        # set mode
        self.mode = ReplicaMode.FOLLOWER
        self._randomize_timeout()
        self.votes = None

        # persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []

        # volatile state
        self.commit_idx = 0
        self.last_applied = 0

        # leader state
        # reset in _become_leader()
        self.next_idx = {}
        self.match_idx = {}


    def _randomize_timeout(self):
        '''
        Updates this socket's timeout to random value. If leader, this timeout
        is pre-emptive of MIN_ELECTION_TIMEOUT by LEADER_PREEMPTIVITY. If not,
        this value is between specified ELECTION_TIMEOUT ranges
        '''
        period = random.uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT) \
            if self.mode != ReplicaMode.LEADER else MIN_ELECTION_TIMEOUT - LEADER_PREEMPTIVITY
        # self.socket.settimeout(timeout)
        self.timeout = time.monotonic() + period


    def _last_log(self) -> LogEntry | None:
        '''
        Get the latest log entry
        '''
        return self.log[:1][0] if len(self.log) else None
    
    def _generate_mid(self, k=8) -> str:
        '''
        Generate random MID for sending new messages
        '''
        return ''.join(random.choices(string.ascii_letters, k=k))


    def _send_hello(self):
        self._send({
            'src': self.id, 
            'dst': BROADCAST_ID, 
            'MID': self._generate_mid(),
            'leader': BROADCAST_ID, 
            'type': 'hello'
        })
    
    
    def _send(self, message: dict):
        # print(f'Sending:\n{json.dumps(message, indent=2)}')
        self.socket.sendto(json.dumps(message).encode(), ('localhost', self.port))
    

    def _reply_to(self, received_message: dict, message_type: str, data: dict):
        # print(f'Replying to {received_message["src"]} ({received_message})')
        self._send({
            'src': self.id, 
            'dst': received_message['src'], 
            'MID': received_message['MID'],
            'leader': self.id if self.mode == ReplicaMode.LEADER else self.voted_for or BROADCAST_ID,
            'type': message_type,
            'value': data
        })
    

    def _send_heartbeat(self):
        '''
        Broadcast AppendEntries as heartbeat
        '''
        if self.mode != ReplicaMode.LEADER:
            raise ValueError('Cannot send heartbeat when not leader')
        
        self._randomize_timeout()

        last_log = self._last_log()

        data = AppendEntriesArgs(
            term=self.current_term,
            leader_id=self.id,
            prev_log_idx=len(self.log),
            prev_log_term=last_log['term'] if last_log else 0,
            entries=[],
            leader_commit_idx=self.commit_idx
        )

        self._send({
            'src': self.id, 
            'dst': BROADCAST_ID, 
            'MID': self._generate_mid(),
            'leader': self.id, 
            'type': MessageType.APPEND_ENTRIES,
            'value': data
        })    


    def _become_follower(self):
        '''
        Set mode to follower
        '''
        print('I am now a follower.')
        self.mode = ReplicaMode.FOLLOWER


    def _become_candidate(self):
        '''
        Increments current_term by 1, sets mode to candidate, and sets votes to 0
        Resets voted_for
        '''
        print('I am now a candidate.')
        self.current_term += 1
        self.mode = ReplicaMode.CANDIDATE
        self.votes = 0

        self.voted_for = None
    

    def _become_leader(self):
        '''
        Sets mode to leader and sends heartbeat
        '''
        print('I am now a leader!')
        self.mode = ReplicaMode.LEADER
        self._send_heartbeat()

        for o in self.others:
            # next_idx[] : (initialized to leader last log index + 1)
            self.next_idx[o] = len(self.log)
            # (initialized to 0, increases monotonically)
            self.match_idx[o] = 0


    def _begin_election(self):
        '''
        Switches to candidate mode, begins election
        '''
        self._become_candidate()
        self._randomize_timeout()

        last_log = self._last_log()

        data = RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.id,
            last_log_idx=len(self.log),
            last_log_term=last_log['term'] if last_log else 0
        )

        self._send({
            'src': self.id,
            'dst': BROADCAST_ID,
            'MID': self._generate_mid(),
            'leader': BROADCAST_ID,
            'type': MessageType.REQUEST_VOTE,
            'value': data
        })


    def _receive_request_vote(self, message: dict) -> tuple[str, dict] | None:
        '''
        Perform RequestVote RPC
        '''
        args: RequestVoteArgs = message['value']

        last_log = self._last_log()

        vote_granted = False
        # Reply false if term < currentTerm (§5.1)
        if args['term'] < self.current_term:
            vote_granted = False
        # If votedFor is null or candidateId, and candidate’s log is at
        # least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        elif (self.voted_for is None or self.voted_for == args['candidate_id']) \
            and args['last_log_term'] >= (last_log['term'] if last_log else 0) \
            and args['last_log_idx'] >= len(self.log):
            vote_granted = True
        # else:
        #     raise ValueError('Could not determine if vote should be granted')

        
        if vote_granted:
            self._randomize_timeout()
            self.voted_for = args['candidate_id']

        return (
            MessageType.REQUEST_VOTE_RES,
            RequestVoteResults(
                term=self.current_term,
                vote_granted=vote_granted
            )
        )
    

    def _receive_vote(self, message: dict) -> tuple[str, dict] | None:
        '''
        Receive a RequestVote result
        '''
        if self.mode != ReplicaMode.CANDIDATE:
            print(f'Received vote while in mode {self.mode}...')
            return None

        result: RequestVoteResults = message['value']

        if result['vote_granted']:
            self.votes += 1
        
        if self.votes > len(self.others) // 2:
            self._become_leader()

        return None
    

    def _receive_append_entries(self, message: dict) -> tuple[str, dict] | None:
        '''
        Receive an AppendEntries RPC
        '''
        args: AppendEntriesArgs = message['value']

        # handle mid-election a leader appearing
        if self.mode == ReplicaMode.CANDIDATE:
            self._become_follower()
        elif self.mode == ReplicaMode.LEADER:
            print('Received AppendEntries as leader...')
            return None
        
        self._randomize_timeout()

        if not isinstance(args, dict):
            print(f'args: {args}')

        prev_log_idx = args['prev_log_idx']
        prev_log_term = args['prev_log_term']
        leader_commit_idx = args['leader_commit_idx']

        success = True
        # Reply false if term < currentTerm 
        if args['term'] < self.current_term:
            success = False
        #  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        elif len(self.log) > prev_log_idx \
            and self.log[prev_log_idx]['term'] != prev_log_term:
                success = False
        
        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it (§5.3)

        # interpretation: (here) we can always throw aray everything after prev_log_idx
        # and then add new entries after!
        if len(self.log) > prev_log_idx:
            self.log = self.log[:prev_log_idx + 1]

        # Append any new entries not already in the log
        self.log.extend(args['entries'])

        #  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if leader_commit_idx > self.commit_idx:
            self.commit_idx = min(leader_commit_idx, len(self.log)) # TODO idx of last entry correct?

        return (
            MessageType.APPEND_ENTRIES_RES,
            AppendEntriesResults(
                term=self.current_term,
                success=success
            )
        )
    

    def _receive_append_entries_results(self, message: dict) -> tuple[str, dict] | None:
        '''
        Receive AppendEntriesResults
        '''
        if self.mode != ReplicaMode.LEADER:
            print(f'Received AppendEntriesResults while in mode {self.mode}...')
            return None
        
        remote = message['src']
        results: AppendEntriesResults = message['value']

        if results['success']:
            # If successful: update nextIndex and matchIndex for follower
            self.next_idx[remote] = len(self.log) # TODO: off by one?
            self.match_idx[remote] = len(self.log) - 1
        else:
            # If AppendEntries fails because of log inconsistency:
            # decrement nextIndex and retry
            self.next_idx[remote] = max(0, self.next_idx[remote] - 1)
            
            # print('------')
            # print(self.next_idx)
            # print(len(self.log))
            
            prev_log_idx = self.next_idx[remote]
            
            if prev_log_idx <= len(self.log):
                return None
            
            prev_log = self.log[prev_log_idx]
            
            data = AppendEntriesArgs(
                term=self.current_term,
                leader_id=self.id,
                prev_log_idx=prev_log_idx,
                prev_log_term=prev_log['term'] if prev_log else 0,
                entries=self.log[prev_log_idx:],
                leader_commit_idx=self.commit_idx
            )
            
            self._send({
                'src': self.id,
                'dst': remote,
                'MID': self._generate_mid(),
                'leader': self.id,
                'type': MessageType.APPEND_ENTRIES,
                'value': data # self.log[self.next_idx[remote]:]
            })
            return None

        # If there exists an N such that N > commitIndex, a majority
        # of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        # set commitIndex = N (§5.3, §5.4)
        while (list(self.match_idx.values()).count(self.commit_idx + 1) > len(self.others) // 2 \
            and self.log[self.commit_idx + 1]['term'] == self.current_term):
            self.commit_idx += 1

        return None


    def _receive_get(self, message: dict) -> tuple[str, dict] | None:
        '''
        Receive a get request
        '''
        if self.mode != ReplicaMode.LEADER:
            # send redirect to leader
            return MessageType.REDIRECT, {}
        
        key = message['key']

        # look backwards through log for last put
        # slow but works for time being
        for entry in reversed(self.log):
            if entry['put_key'] == key:
                return MessageType.OK, entry['put_value']
        
        return MessageType.FAIL
    

    def _receive_put(self, message: dict) -> tuple[str, dict] | None:
        '''
        Receive a put request
        '''
        if self.mode == ReplicaMode.FOLLOWER:
            # send redirect to leader
            return MessageType.REDIRECT, {}
        
        key = message['key']
        value = message['value']

        self.log.append(LogEntry(
            term=self.current_term,
            put_key=key,
            put_value=value
        ))

        return MessageType.OK, {}


    def _handle_timeout(self):
        '''
        Handle either sending heartbeats or beginning an election
        '''
        if self.mode == ReplicaMode.LEADER:
            self._send_heartbeat()
        else:
            self._begin_election()


    def run(self):
        while True:
            if time.monotonic() >= self.timeout:
                self._handle_timeout()

            try:
                data, addr = self.socket.recvfrom(65535)
                message = json.loads(data.decode())

                result = {
                    MessageType.GET: self._receive_get,
                    MessageType.PUT: self._receive_put,
                    MessageType.FAIL: lambda x: None,
                    MessageType.HELLO: lambda x: None,
                    MessageType.REQUEST_VOTE: self._receive_request_vote,
                    MessageType.REQUEST_VOTE_RES: self._receive_vote,
                    MessageType.APPEND_ENTRIES: self._receive_append_entries,
                    MessageType.APPEND_ENTRIES_RES: self._receive_append_entries_results
                }[message['type']](message)

                if result is not None and len(result) == 2:
                    message_type, value = result
                    self._reply_to(message, message_type, value)

            except TimeoutError as e:
                pass




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=int)
    parser.add_argument('id', type=str)
    parser.add_argument('others', type=str, nargs='+')

    args = parser.parse_args()
    replica = Replica(args.port, args.id, args.others)
    replica.run()
