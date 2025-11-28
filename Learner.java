package comp512st.paxos;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Learner for Multi-Paxos with sequence numbers (slots).
 * Keeps committed values in order and delivers them sequentially to the application.
 */
public class Learner {

    private String myProcess;

    // seq -> committed value
    private final ConcurrentHashMap<Integer, Move> committedValues = new ConcurrentHashMap<>();

    // The next slot to deliver to the application layer
    private int nextSlotToDeliver = 0;

    // The highest decided/confirmed slot so far
    private int highestCommittedSeq = -1;

    // Delivery queue for the application
    private final BlockingQueue<Object> deliverQueue = new LinkedBlockingQueue<>(); //Object

    private final Object stateLock = new Object();

    private final int HISTORY = 1000;

    /**
     * Called when a CONFIRM message arrives from proposer/acceptor.
     * Stores and delivers values in order.
     */
    public void confirm(PaxosMsg msg, int seq) {
        if (msg == null || seq < 0) return;

        synchronized (stateLock) {
            if (committedValues.containsKey(seq)) {
                // Already have it, ignore duplicates
                return;
            }

            committedValues.put(seq, msg.value);
            if (seq > highestCommittedSeq) highestCommittedSeq = seq;
            System.out.printf("[Learner] Learner commit value=%s to seq=%d\n",
                    msg.value, seq);

            // Try to deliver all consecutive values starting from nextSlotToDeliver
            deliverInOrder();
        }
    }

    /**
     * Deliver any pending consecutive values starting from nextSlotToDeliver.
     */
    private void deliverInOrder() {
        while (committedValues.containsKey(nextSlotToDeliver)) {
            Object val = committedValues.get(nextSlotToDeliver).val;
//            Object[] moveInfo = (Object[]) val;
//            int playerNum = (Integer) moveInfo[0];
//            char direction = (Character) moveInfo[1];

            //Move m = new Move(playerNum, direction);
            deliverQueue.offer(val);

            Iterator<Integer> it = committedValues.keySet().iterator();
            int minSlotToKeep = nextSlotToDeliver - HISTORY;
            while (it.hasNext()) {
                int slot = it.next();
                if (slot < minSlotToKeep) {
                    it.remove();
                }
            }
            //committedValues.remove(nextSlotToDeliver);
            nextSlotToDeliver++;
        }
    }

    /**
     * Called by application to take the next decided move (blocking).
     */
    public Object takeNext() throws InterruptedException {
        return deliverQueue.take();
    }

    /**
     * @return The next sequence number that is not yet committed.
     *         Used by proposers to know which slot to start proposing for.
     */
    public synchronized int getNextUncommittedSeq() {
        // next uncommitted = highest committed + 1
        return highestCommittedSeq + 1;
    }

    /**
     * @return Highest committed sequence so far.
     */
    public synchronized int getHighestDecidedSeq() {
        return highestCommittedSeq;
    }

    /**
     * For debugging/logging.
     */
    public synchronized String getState() {
        return String.format("LearnerState{nextDeliver=%d, highestCommitted=%d, committedKeys=%s, queueSize=%d}",
                nextSlotToDeliver, highestCommittedSeq, committedValues.keySet(), deliverQueue.size());
    }

    public Move getCommittedMove(int seq) {
        return committedValues.get(seq);
    }
}