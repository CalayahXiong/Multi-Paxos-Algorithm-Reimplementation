package comp512st.paxos;

import comp512.gcl.GCL;
import comp512.utils.FailCheck;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Proposer {

    private int ballotID;
    private int highestRefusedBID;
    private int majority;

    private BlockingQueue<PaxosMsg> promises = new LinkedBlockingQueue<>();
    private BlockingQueue<PaxosMsg> refuses = new LinkedBlockingQueue<>();
    private BlockingQueue<PaxosMsg> accepted = new LinkedBlockingQueue<>();

    public boolean isLeader;
    private String myProcess;
    private String[] allGroupProcesses;

    private final int myProcessId;
    private int round;

    private Move pendingValue = null;

    public Proposer(String myProcess, String[] allGroupProcesses){
        this.ballotID = 0;
        this.highestRefusedBID = -1;
        this.majority = allGroupProcesses.length/2 + 1;
        this.myProcess = myProcess;
        this.allGroupProcesses = allGroupProcesses;
        this.isLeader = false;
        this.myProcessId = calculateProcessId(myProcess, allGroupProcesses);
        this.round = 0;
    }

    private int calculateProcessId(String myProcess, String[] allProcesses) {
        for (int i = 0; i < allProcesses.length; i++) {
            if (allProcesses[i].equals(myProcess)) {
                System.out.println("[Proposer] Assigned processId=" + i + " for " + myProcess);
                return i;
            }
        }
        return -1;
    }
    /** Receive promise messages */
    public void onPromise(PaxosMsg msg) {
        promises.offer(msg);
    }

    /** Receive refuse messages */
    public void onRefuse(PaxosMsg msg){
        refuses.offer(msg);
    }

    /** Receive accept ack */
    public void onAcceptAck(PaxosMsg msg) {
        accepted.offer(msg);
    }

    /**
     * Propose a value at a given sequence number.
     * If stable leader, we can skip Phase 1 (prepare/propose).
     */
    public boolean onPropose(GCL gcl, FailCheck failCheck, Move val, int seq) {

        if(pendingValue == null) pendingValue = val;
        Move proposedValue = pendingValue;

        //int seq = nextSeq++; // assign sequence number
        int baseBallotID = (round * allGroupProcesses.length) + myProcessId; //3 VS 2
        ballotID = Math.max(baseBallotID, highestRefusedBID + 1);

        int thisBallot = ballotID;
        isLeader = false;
        round++;

        System.out.println("[Proposer] Starting Paxos round seq=" + seq +
                " baseBallot=" + baseBallotID +
                " highestRefusedId=" + highestRefusedBID +
                " allProcess.length=" + allGroupProcesses.length +
                " ballotID=" + ballotID +
                " (myId=" + myProcessId + ", round=" + round + ")" +
                " value=" + proposedValue);
        // 1: PROPOSE
        PaxosMsg proposeMsg = PaxosMsg.makePropose(ballotID, myProcess, seq);
        gcl.broadcastMsg(proposeMsg);
        // FAILCHECK 3: invoked immediately AFTER a process sends out its proposal to become a leader
        //failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);
        System.out.println("[Proposer] PROPOSE broadcast sent for seq=" + seq);

        // Wait for majority promises or refuses
        List<PaxosMsg> promiseList = collectMajority(promises, PaxosMsg.Type.PROMISE, thisBallot, seq);
        if(promiseList.size() < majority) {
            // check refuses for highest refused bid
            int highest = highestRefusedBID;
            for(PaxosMsg msg : refuses){
                highest = Math.max(highest, msg.ballotID);
                System.out.println("[Proposer] Received REFUSE from " + msg.sender + " maxBID=" + msg.ballotID + " seq=" + msg.seq);
            }
            highestRefusedBID = Math.max(highestRefusedBID, highest);
            System.out.println("[Proposer] Did NOT receive majority PROMISEs for seq=" + seq + ". Will retry.");
            return false;
        }

        isLeader = true;
        // FAILCHECK 4: invoked immediately AFTER a process sees that it has been accepted by the majority as the leader.
        //failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);
        System.out.println("[Proposer] Received majority PROMISEs for seq=" + seq + ". Becoming leader.");

        // 2: choose value
        PaxosMsg highestAccept = null;
        for(PaxosMsg p : promiseList){
            if(p.acceptBID >= 0 && (highestAccept == null || p.acceptBID > highestAccept.acceptBID)) highestAccept = p;
        }
        Move finalValue = (highestAccept == null) ? proposedValue : highestAccept.value;
        System.out.println("[Proposer] Chosen value for seq=" + seq + ": " + finalValue);

        // 2: ACCEPT_REQUEST
        PaxosMsg acceptReq = PaxosMsg.makeAcceptReq(ballotID, finalValue, myProcess, seq);
        gcl.broadcastMsg(acceptReq);
        System.out.println("[Proposer] ACCEPT_REQUEST broadcast sent for seq=" + seq);

        // Wait for majority ACCEPT_ACK
        List<PaxosMsg> ackList = collectMajority(accepted, PaxosMsg.Type.ACCEPT_ACK, thisBallot, seq);
        if (ackList.size() < majority) {
            System.out.println("[Proposer] I DID NOT receive majority ACCEPT_ACKs for seq=" + seq + ".");
            // record any refuses seen meanwhile
            int highest = highestRefusedBID;
            PaxosMsg r;
            while ((r = refuses.poll()) != null) {
                if (r.seq == seq) highest = Math.max(highest, r.ballotID);
            }
            highestRefusedBID = Math.max(highestRefusedBID, highest);
            return false;
        }

        // 3: CONFIRM
        // FAILCHECK 5: invoked immediately by the process once a majority has accepted its proposed value
        //failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
        PaxosMsg confirm = PaxosMsg.makeConfirm(ballotID, finalValue, myProcess, seq);
        gcl.broadcastMsg(confirm);
        System.out.println("[Proposer] Value COMMITTED for seq=" + seq + ": " + finalValue);

        drainMatching(promises, seq, thisBallot);
        drainMatching(accepted, seq, thisBallot);
        drainMatching(refuses, seq, thisBallot);
        pendingValue = null;

        //Object[] fv = (Object[]) finalValue;
        //Object[] pv = (Object[]) proposedValue;
        //return new ProposalResponse(true, fv[0] == pv[0] && fv[1] == pv[1]); //have to compare both proposer and value

        boolean myValueCommitted = proposedValue.equals(finalValue);
        return myValueCommitted;
    }

    private List<PaxosMsg> collectMajority(BlockingQueue<PaxosMsg> queue, PaxosMsg.Type expectedType, int ballot, int seq){
        List<PaxosMsg> collected = new ArrayList<>();
        long start = System.currentTimeMillis();
        long timeout = 3000;
        while ((System.currentTimeMillis() - start) < timeout) {
            PaxosMsg msg = queue.poll();
            if (msg == null) {
                // slight sleep to avoid busy loop
                try { Thread.sleep(5); } catch (InterruptedException ignored) {}
            } else {
                // filter by type, seq, and ballot where appropriate
                if (msg.type != expectedType) continue;
                if (msg.seq != seq) continue;
                // For PROMISE, ensure msg.ballotID == ballot (promise replies use prepare's ballot)
                // For ACCEPT_ACK, the ack should carry the same ballot that was accepted
                if (msg.ballotID != ballot) continue;

                collected.add(msg);
                System.out.printf("[collectMajority] got %s from %s, total=%d/%d\n",
                        msg.type, msg.sender, collected.size(), majority);
                if (collected.size() >= majority) return collected;
            }
        }
        return collected; // timeout / not enough responses
    }

    private void drainMatching(BlockingQueue<PaxosMsg> queue, int seq, int ballot) {
        List<PaxosMsg> keep = new ArrayList<>();
        queue.drainTo(keep);
        for (PaxosMsg m : keep) {
            boolean drop = false;

            if (m.seq <= seq) {
                // For refuses: clear all before-seq messages regardless of ballot
                if (m.type == PaxosMsg.Type.REFUSE) {
                    drop = true;
                }
                // For other types: only clear before-seq + before-ballot messages
                else if (m.ballotID <= ballot) {
                    drop = true;
                }
            }

            if (!drop) queue.offer(m);
        }
    }
}