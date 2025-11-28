package comp512st.paxos;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Acceptor {

    private String myProcess;

    // Track promises and accepted values per sequence number
    private Map<Integer, Integer> maxBIDs;    // seq -> max promised ballotID
    private Map<Integer, Integer> acceptBIDs; // seq -> accepted ballotID
    private Map<Integer,Move> values;      // seq -> accepted value

    private final Map<Integer, Boolean> decided;     // seq -> true if decided

    public Acceptor(String myProcess) {
        this.myProcess = myProcess;
        maxBIDs = new HashMap<>();
        acceptBIDs = new HashMap<>();
        values = new HashMap<>();
        decided = new HashMap<>();
    }

    // Upon receiving PROPOSE(ballotID, seq)
    public PaxosMsg onPromise(PaxosMsg msg) {
        int seq = msg.seq;
        int ballotID = msg.ballotID;
        String sender = myProcess;

        // CASE 1: Already decided
        if (decided.getOrDefault(seq, false)) {
            Object val = values.get(seq);
            System.out.printf("[Acceptor] seq=%d already decided with value=%s, reject new promise (ballot=%d)%n",
                    seq, val, ballotID);
            return PaxosMsg.makeRefuse(ballotID, sender, seq); // or a specialized DECIDED reply
        }

        int maxBID = maxBIDs.getOrDefault(seq, -1);
        int acceptedBallot = acceptBIDs.getOrDefault(seq, -1);
        Move val = values.get(seq);

        // CASE 2: Lower ballotID — reject
        if (ballotID <= maxBID) {
            System.out.printf("[Acceptor] Refuse seq=%d for lower ballotID=%d < maxBID=%d%n", seq, ballotID, maxBID);
            return PaxosMsg.makeRefuse(maxBID, sender, seq);
        }

        // CASE 3: Normal promise
        maxBIDs.put(seq, ballotID);
        System.out.printf("[Acceptor] PROMISE seq=%d ballotID=%d%n", seq, ballotID);

        // Return prior accepted value if exists
        if (val == null)
            return PaxosMsg.makePromise(ballotID, -1, null, sender, seq);
        else
            return PaxosMsg.makePromise(ballotID, acceptedBallot, val, sender, seq);
    }


    // Upon receiving ACCEPT_REQUEST(ballotID, value, seq)
    public PaxosMsg onAcceptRequest(PaxosMsg msg) {
        int seq = msg.seq;
        int ballotID = msg.ballotID;
        Move proposedVal = msg.value;
        String sender = myProcess;

        if (decided.getOrDefault(seq, false)) {
            Object val = values.get(seq);
            System.out.printf("[Acceptor] seq=%d already decided on values=%s, ignore acceptRequest (ballot=%d)%n",
                    seq, Arrays.toString((Object[]) val), ballotID);
            return PaxosMsg.makeDeny(ballotID, sender, seq); // or makeAlreadyDecided()
        }

        int maxBID = maxBIDs.getOrDefault(seq, -1);

        if (ballotID < maxBID) { //there are chances for two proposers proposing the same ballotID in the same round
            System.out.printf("[Acceptor] Reject acceptRequest seq=%d, ballot=%d < promised=%d%n",
                    seq, ballotID, maxBID);
            return PaxosMsg.makeDeny(maxBID, sender, seq);
        }

        maxBIDs.put(seq, ballotID);   // update promise
        acceptBIDs.put(seq, ballotID);
        values.put(seq, proposedVal);

        System.out.printf("[Acceptor] ACCEPT seq=%d ballot=%d value=%s%n",
                seq, ballotID, proposedVal);

        return PaxosMsg.makeAcceptAck(ballotID, proposedVal, sender, seq);
    }


    // Called when the value has been decided for a sequence number
    public void onDecided(int seq, Object finalVal) {
        Object val = values.get(seq);
        if (val != null && val.equals(finalVal)) {
            decided.put(seq, true);
            values.remove(seq);
            acceptBIDs.remove(seq);
            maxBIDs.remove(seq); // optionally, remove promise info
        }
    }

}