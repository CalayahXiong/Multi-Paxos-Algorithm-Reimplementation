package comp512st.paxos;

import java.io.Serializable;

class PaxosMsg implements Serializable {

    public enum Type { PROPOSE, PROMISE, ACCEPT_REQUEST, ACCEPT_ACK, CONFIRM, REFUSE, DENY }

    public Type type;
    public int ballotID;
    public int acceptBID;
    public Move value; //Move
    public String sender;
    public int seq; // sequence number for Multi-Paxos

    public PaxosMsg(Type type, int ballotID, int acceptBID, Move value, String sender, int seq) {
        this.type = type;
        this.ballotID = ballotID;
        this.acceptBID = acceptBID;
        this.value = value;
        this.sender = sender;
        this.seq = seq;
    }

    // PROPOSE message
    public static PaxosMsg makePropose(int ballotID, String sender, int seq) {
        return new PaxosMsg(Type.PROPOSE, ballotID, -1, null, sender, seq);
    }

    // ACCEPT_REQUEST message
    public static PaxosMsg makeAcceptReq(int ballotID, Move value, String sender, int seq) {
        return new PaxosMsg(Type.ACCEPT_REQUEST, ballotID, -1, value, sender, seq);
    }

    // REFUSE message
    public static PaxosMsg makeRefuse(int maxBID, String sender, int seq) {
        return new PaxosMsg(Type.REFUSE, maxBID, -1, null, sender, seq);
    }

    // DENY message
    public static PaxosMsg makeDeny(int ballotID, String sender, int seq) {
        return new PaxosMsg(Type.DENY, ballotID, -1, null, sender, seq);
    }

    // PROMISE message
    public static PaxosMsg makePromise(int ballotID, int acceptBID, Move value, String sender, int seq) {
        return new PaxosMsg(Type.PROMISE, ballotID, acceptBID, value, sender, seq);
    }

    // ACCEPT_ACK message
    public static PaxosMsg makeAcceptAck(int ballotID, Move value, String sender, int seq) {
        return new PaxosMsg(Type.ACCEPT_ACK, ballotID, -1, value, sender, seq);
    }

    // CONFIRM message
    public static PaxosMsg makeConfirm(int ballotID, Move value, String sender, int seq) {
        return new PaxosMsg(Type.CONFIRM, ballotID, -1, value, sender, seq);
    }
}