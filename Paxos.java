package comp512st.paxos;

// Access to the GCL layer

import comp512.gcl.GCL;
import comp512.gcl.GCMessage;
import comp512.utils.FailCheck;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos {
	GCL gcl;
	FailCheck failCheck;

	String myProcess;
	String[] allGroupProcesses;
	Logger logger;

	private Proposer proposer;
	private Acceptor acceptor;
	private Learner learner;
	private Object broadcastLock;

	private volatile boolean running;
	private Thread listenerThread;

	private Long requestID;

	// Multi-Paxos: maintain sequence number for each value
	//private final AtomicInteger nextSeqToPropose;

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck)
			throws IOException, UnknownHostException {
		this.failCheck = failCheck;
		this.myProcess = myProcess;
		this.allGroupProcesses = allGroupProcesses;
		this.logger = logger;

		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger);

		this.proposer = new Proposer(myProcess, allGroupProcesses);
		this.acceptor = new Acceptor(myProcess);
		this.learner = new Learner();

		broadcastLock = new Object();

		//nextSeqToPropose = new AtomicInteger(0);

		listenerThread = new Thread(this::listenLoop, "Paxos-Listener");
		listenerThread.setDaemon(true);
		running = true;
		listenerThread.start();

		requestID = Long.valueOf(0);
	}

	private void listenLoop() {
		while (running) {
			try {
				GCMessage gcmsg = gcl.readGCMessage(); // blocks
				if (gcmsg == null) continue;

				Object raw = gcmsg.val;
				if (!(raw instanceof PaxosMsg)) continue;

				PaxosMsg msg = (PaxosMsg) raw;

				switch (msg.type) {
					case PROPOSE: {
						PaxosMsg promise = acceptor.onPromise(msg);
						gcl.sendMsg(promise, msg.sender);
						// FAILCHECK 2: invoked immediately AFTER a process sends out its vote (promise or refuse) for leader election
						//failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
						break;
					}
					case PROMISE: {
						proposer.onPromise(msg);
						break;
					}
					case ACCEPT_REQUEST: {
						PaxosMsg ack = acceptor.onAcceptRequest(msg);
						if (ack != null) gcl.sendMsg(ack, msg.sender);
						break;
					}
					case ACCEPT_ACK: {
						proposer.onAcceptAck(msg);
						break;
					}
					case CONFIRM: {
						System.out.println("[Paxos] Received CONFIRM for seq=" + msg.seq +
								" from " + msg.sender +
								", value=" + msg.value);
						learner.confirm(msg, msg.seq);  // Learner will handle delivering in order
						acceptor.onDecided(msg.seq, msg.value);  // Acceptor updates its per-slot state
						//System.out.println("[Paxos] After CONFIRM processing: " + learner.getState());
						break;
					}
					case REFUSE: {
						proposer.onRefuse(msg);
						break;
					}
					default:
						logger.warning("Unknown Paxos message type: " + msg.type);
				}

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				running = false;
				break;
			} catch (Exception ex) {
				logger.log(Level.SEVERE, "Exception in Paxos.listenLoop", ex);
			}
		}
	}
	// This is what the application layer is going to call to send a message/value, such as the player and the move
	// Propose -> Accept? -> Confirm
	public void broadcastTOMsg(Object val) {

		synchronized (broadcastLock) {
			//generate a requestID for this move
			long id = ++requestID;
			Move move = new Move(id, val);

			boolean myValueCommitted = false;
			long baseSleep = 200;
			int attempt = 0;

			while (!myValueCommitted) { //keep proposing for my value in next seq
				int seq = learner.getNextUncommittedSeq();
				//boolean committed = false;
				attempt++;

				System.out.printf("\n[Paxos] Starting attempt %d broadcast for value=%s at seq=%d\n",
						attempt, move, seq);

				try {
					//boolean response = proposer.onPropose(gcl, failCheck, val, seq);
					myValueCommitted = proposer.onPropose(gcl, failCheck, move, seq);
					//committed = proposeSucceeded;
					if(myValueCommitted) return;  //it is me who committed my move

					int nowHighest = learner.getHighestDecidedSeq();
					if (nowHighest >= seq) {
						System.out.printf("[Paxos] Learner caught up to seq=%d (someone committed). Moving on...\n", nowHighest);
						//not me, someone else committed my move?
						Move m = learner.getCommittedMove(seq);
						// has to judge here m!=null
						// since leaner may hs already updated the HighestDecidedSeq but also has removed val in seq out of committedMove
						if (m == null || Objects.deepEquals(m.val, val) && m.requestID == id) {
							return;
						} else {
							continue;
						}
					} else {
						long backoff = Math.min(5000, (long) (baseSleep * Math.pow(2, attempt)));
						System.out.printf("[Paxos] Attempt #%d failed (seq=%d). Retrying in %d ms...\n",
								attempt, seq, backoff);
						sleepQuietly(backoff);
					}

				} catch (Exception e) {
					long backoff = Math.min(5000, (long) (baseSleep * Math.pow(2, attempt)));
					System.out.printf("[Paxos] Error during attempt #%d, sleeping %d ms before retry...\n",
							attempt, backoff);
					e.printStackTrace();
					sleepQuietly(backoff);
				}
			}
		}
	}



	private void sleepQuietly(long ms) {
		try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	// Learner receive Confirm then call this function
	public Object acceptTOMsg() throws InterruptedException
	{
		// This is just a place holder.
		return learner.takeNext();
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		running = false;
		listenerThread.interrupt();
		gcl.shutdownGCL();
	}
}