/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;
    
    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */
    //从peer收到的消息
    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         *
         */
        // 投票给的 Leader ，提议的 Leader 的sid
        long leader;

        /*
         * zxid of the proposed leader
         */
        //投票给的 Leader 的zxid，提议的 Leader 的zxid
        long zxid;

        /*
         * Epoch
         */
        // 发送者的 logicClock  逻辑时钟
        long electionEpoch;

        /*
         * current state of sender
         */
        // 发送者的状态
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        // 发送者的sid
        long sid;
        
        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */
        // 投票给的 Leader 的 epoch
        long peerEpoch;
    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
                long leader,
                long zxid,
                long electionEpoch,
                ServerState state,
                long sid,
                long peerEpoch,
                byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */
        byte[] configData = dummyData;

        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread  {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        //从接收队列中弹出数据
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if(response == null) continue;

                        final int capacity = response.buffer.capacity();

                        // The current protocol and two previous generations all send at least 28 bytes
                        if (capacity < 28) {
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (capacity == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);

                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null;

                        try {

                            if (!backCompatibility28) {
                                rpeerepoch = response.buffer.getLong();
                                if (!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */

                                    version = response.buffer.getInt();
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }


                            // check if we have a version that includes config. If so extract config info from message.
                            if (version > 0x1) {
                                int configLength = response.buffer.getInt();

                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if (configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format("Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d",
                                                          response.sid, capacity, version, configLength));
                                }

                                byte b[] = new byte[configLength];

                                response.buffer.get(b);

                                synchronized (self) {
                                    try {
                                        rqv = self.configFromString(new String(b));
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        if (rqv.getVersion() > curQV.getVersion()) {
                                            LOG.info("{} Received version: {} my version: {}", self.getId(),
                                                    Long.toHexString(rqv.getVersion()),
                                                    Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            if (self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                self.processReconfig(rqv, null, null, false);
                                                if (!rqv.equals(curQV)) {
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();

                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch (IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}. " +
                                                  "Continue to process the notification message without handling the configuration.", response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch (BufferUnderflowException | IOException e) {
                            LOG.warn("Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})",
                                     response.sid, capacity, e);
                            continue;
                        }

                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if(!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch(),
                                    qv.toString().getBytes());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = "
                                        + self.getId());
                            }

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }

                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            if(LOG.isInfoEnabled()){
                                printNotification(n);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if(self.getPeerState() == QuorumPeer.ServerState.LOOKING){
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock.get())){
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if(ackstate == QuorumPeer.ServerState.LOOKING){
                                    if(LOG.isDebugEnabled()){
                                        LOG.debug("Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                                self.getId(),
                                                response.sid,
                                                Long.toHexString(current.getZxid()),
                                                current.getId(),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));
                                    }

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            self.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        /**
         * 选举组件的 网络发送线程
         */
        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager){
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        //拿到消息
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if(m == null) continue;
                        //处理,放到CnxManager的SendQueue中
                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                //构造消息体
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(),
                                                    m.leader,
                                                    m.zxid,
                                                    m.electionEpoch,
                                                    m.peerEpoch,
                                                    m.configData);

                // 构造BlockingQueue，然后以sid为key，将queue放到queueSendMap里
                manager.toSend(m.sid, requestBuffer);

            }
        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws,
                    "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr,
                    "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start(){
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt(){
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock(){
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state,
            long leader,
            long zxid,
            long electionEpoch,
            long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state,
            long leader,
            long zxid,
            long electionEpoch,
            long epoch,
            byte[] configData) {
        byte requestBytes[] = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager){
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        if(LOG.isDebugEnabled()){
            LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
                v.getId(), Long.toHexString(v.getZxid()), self.getId(), self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager(){
        return manager;
    }

    volatile boolean stop;
    public void shutdown(){
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /** Send notifications to all peers upon a change in our vote */
    private void sendNotifications() {
        // 所有的节点都需要发送
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            ToSend notmsg =
                    new ToSend(
                            ToSend.mType.notification,
                            // 投票的leader
                            proposedLeader,
                            // 我投票给的 Leader 的zxid
                            proposedZxid,
                            // 逻辑时钟，代表轮次
                            logicalclock.get(),
                            // 表示当前节点的状态为寻主
                            QuorumPeer.ServerState.LOOKING,
                            // 接收消息节点的sid
                            sid,
                            proposedEpoch,
                            qv.toString().getBytes());
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Sending Notification: "
                                + proposedLeader
                                + " (n.leader), 0x"
                                + Long.toHexString(proposedZxid)
                                + " (n.zxid), 0x"
                                + Long.toHexString(logicalclock.get())
                                + " (n.round), "
                                + sid
                                + " (recipient), "
                                + self.getId()
                                + " (myid), 0x"
                                + Long.toHexString(proposedEpoch)
                                + " (n.peerEpoch)");
            }
            // 发送给发送队列，异步发送
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n){
        LOG.info("Notification: "
                + Long.toHexString(n.version) + " (message format version), "
                + n.leader + " (n.leader), 0x"
                + Long.toHexString(n.zxid) + " (n.zxid), 0x"
                + Long.toHexString(n.electionEpoch) + " (n.round), " + n.state
                + " (n.state), " + n.sid + " (n.sid), 0x"
                + Long.toHexString(n.peerEpoch) + " (n.peerEPoch), "
                + self.getPeerState() + " (my state)"
                + (n.qv!=null ? (Long.toHexString(n.qv.getVersion()) + " (n.config version)"):""));
    }


    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param id    Server identifier
     * @param zxid  Last zxid observed by the issuer of this vote
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if(self.getQuorumVerifier().getWeight(newId) == 0){
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */
        //先对比 epoch，优先选择 epoch 大的；如果 epoch 一样，那再对比 zxid，优先选 zxid 大的；如果 zxid 也一样，那就选一个 myid 最大的
        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                        ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have
     * sufficient to declare the end of the election round.
     * 
     * @param votes
     *            Set of votes
     * @param vote
     *            Identifier of the vote received last
     */
    //判断票是否过半
    protected boolean termPredicate(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null
                && self.getLastSeenQuorumVerifier().getVersion() > self
                        .getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            //如果是自己本地的票则直接ack
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet.hasAllQuorums();
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(
            Map<Long, Vote> votes,
            long leader,
            long electionEpoch){

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(leader != self.getId()){
            if(votes.get(leader) == null) predicate = false;
            else if(votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        } else if(logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch){
        if(LOG.isDebugEnabled()){
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                    + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                    + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        //认为Leader的sid
        proposedLeader = leader;
        // 我投票给的 Leader 的zxid
        proposedZxid = zxid;
        // 我投票给的 Leader 的 epoch
        proposedEpoch = epoch;
    }

    synchronized public Vote getVote(){
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT){
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        }
        else{
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId(){
        if(self.getQuorumVerifier().getVotingMembers().containsKey(self.getId()))       
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
        	try {
        		return self.getCurrentEpoch();
        	} catch(IOException e) {
        		RuntimeException re = new RuntimeException(e.getMessage());
        		re.setStackTrace(e.getStackTrace());
        		throw re;
        	}
        else return Long.MIN_VALUE;
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
            //记录开始投票的时间
           self.start_fle = Time.currentElapsedTime();
        }

        try {
            //票箱，存储收到的投票
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();

            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = finalizeWait;

            synchronized(this){
                //更新逻辑时钟
                logicalclock.incrementAndGet();
                //认为自己是主
                //先投自己一票    myId            zxid                   epoch
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() +
                    ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            //异步将投票信息发送给其他节点
            sendNotifications();

            /*
             * Loop in which we exchange notifications until we find a leader
             */

            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                // 从响应队列中拉出票
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                // 响应队列超过阻塞时间会返回null
                if (n == null) {
                    // 之前的票是否都发送出去了
                    if (manager.haveDelivered()) {
                        // 重新发送
                        sendNotifications();
                    } else {
                        // 这里才触发首次连接
                        // 与每个建立连接
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff 做一次退避
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout =
                            (tmpTimeOut < maxNotificationInterval
                                    ? tmpTimeOut
                                    : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                } else if (validVoter(n.sid) && validVoter(n.leader)) {
                    // 正常拿到票 首先做校验，判断收到选票的sid和选举 Leader 的sid是否在我们所配置的集群myid列表内。
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     */
                    // 判断接收到投票者的状态
                    switch (n.state) {
                            // 如果投票者的状态是LOOKING，代表也是寻主状态，需要参与投票进行选主
                        case LOOKING:
                            // If notification > current, replace and send messages out
                            // 接受到的logicClock大于自己的logicClock
                            // 这里感觉是说选举的轮次？，如果别人的大，就特殊处理
                            if (n.electionEpoch > logicalclock.get()) {
                                // 把自己的逻辑时钟更新为收到的
                                logicalclock.set(n.electionEpoch);
                                // 清空票箱
                                recvset.clear();
                                // 比较epoch、zxid、myid
                                if (totalOrderPredicate(
                                        n.leader,
                                        n.zxid,
                                        n.peerEpoch,
                                        getInitId(),
                                        getInitLastLoggedZxid(),
                                        getPeerEpoch())) {
                                    // 把自己的票据更新为对方的
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    // 如果收到的zid等较小，则认为自己是主
                                    updateProposal(
                                            getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                                }
                                // 将新的票据发送给其他节点,重新投票
                                sendNotifications();

                                // 接收到的 logicClock 小于自己的 logicClock，忽略
                            } else if (n.electionEpoch < logicalclock.get()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(
                                            "Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                                    + Long.toHexString(n.electionEpoch)
                                                    + ", logicalclock=0x"
                                                    + Long.toHexString(logicalclock.get()));
                                }
                                // 说明收到的票过期了，直接忽略，拉取下一张票
                                break;

                                // 接收到的 logicClock 等于自己的 logicClock
                                // 对比被推荐节点和自己的信息，进去if的话说明对方的票牛逼
                            } else if (totalOrderPredicate(
                                    n.leader,
                                    n.zxid,
                                    n.peerEpoch,
                                    proposedLeader,
                                    proposedZxid,
                                    proposedEpoch)) {

                                // 更新本地的投票信息为别人的
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                // 把最新的选票发送出去
                                sendNotifications();
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                        "Adding vote: from="
                                                + n.sid
                                                + ", proposed leader="
                                                + n.leader
                                                + ", proposed zxid=0x"
                                                + Long.toHexString(n.zxid)
                                                + ", proposed election epoch=0x"
                                                + Long.toHexString(n.electionEpoch));
                            }

                            // don't care about the version if it's in LOOKING state
                            // 将收到的票放入信箱
                            recvset.put(
                                    n.sid,
                                    new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                            // 判断票数是否过半,这里要把自己本地的票也传进去
                            if (termPredicate(
                                    recvset,
                                    new Vote(
                                            proposedLeader,
                                            proposedZxid,
                                            logicalclock.get(),
                                            proposedEpoch))) {

                                // Verify if there is any change in the proposed leader
                                // 因为过半，符合 Leader 选举原则，因此处理一些选举逻辑
                                // 1、轮询接收队列，
                                //   case1:如果新的票牛逼，则塞回接受队列，并结束循环
                                //   case:超过超时时间仍然没拿到，此时n为null
                                while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS))
                                        != null) {
                                    if (totalOrderPredicate(
                                            n.leader,
                                            n.zxid,
                                            n.peerEpoch,
                                            proposedLeader,
                                            proposedZxid,
                                            proposedEpoch)) {
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                                /*
                                 * This predicate is true once we don't read any new
                                 * relevant message from the reception queue
                                 */
                                // 说明没有更加牛逼的票
                                if (n == null) {
                                    // 看下投票结果自己是不是 Leader ，如果自己是 Leader ，那就把自己的状态从 LOOKING 改为
                                    // LEADING
                                    // learningState 判断自己是Follower还是OBSERVING
                                    self.setPeerState(
                                            (proposedLeader == self.getId())
                                                    ? ServerState.LEADING
                                                    : learningState());
                                    // 组装这次 Leader 选举最终的投票结果
                                    Vote endVote =
                                            new Vote(
                                                    proposedLeader,
                                                    proposedZxid,
                                                    logicalclock.get(),
                                                    proposedEpoch);

                                    // 清空自己接收投票信息的队列，因为都选出来了嘛，所以接收队列里的票也无效了，因此清空下无效投票。
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            break;

                        case OBSERVING:
                            LOG.debug("Notification from observer: " + n.sid);
                            break;
                        case FOLLOWING:
                            // following和leading走一样的逻辑

                            // 如果对方是Leader
                        case LEADING:
                            /*
                             * Consider all notifications from the same epoch
                             * together.
                             */
                            // 对方的逻辑时间戳和自己的相等
                            if (n.electionEpoch == logicalclock.get()) {
                                // 存放到票箱
                                recvset.put(
                                        n.sid,
                                        new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                                // 判断是否过半
                                if (termPredicate(
                                                recvset,
                                                new Vote(
                                                        n.version,
                                                        n.leader,
                                                        n.zxid,
                                                        n.electionEpoch,
                                                        n.peerEpoch,
                                                        n.state))
                                        && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                    // 过半，则设置值，和上面的操作一样
                                    self.setPeerState(
                                            (n.leader == self.getId())
                                                    ? ServerState.LEADING
                                                    : learningState());
                                    Vote endVote =
                                            new Vote(
                                                    n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            // 能走到下面有两种情况
                            // case1 收到票的逻辑时间戳和自己不相等
                            // case2 收到了票，但是没有过半

                            /*
                             * Before joining an established ensemble, verify that
                             * a majority are following the same leader.
                             *
                             * 加入一个集群中，则需要验证其他节点也是跟随这个Leader(防止脑裂？)
                             */
                            // todo 这里需要再看下，不太理解在做什么
                            outofelection.put(
                                    n.sid,
                                    new Vote(
                                            n.version,
                                            n.leader,
                                            n.zxid,
                                            n.electionEpoch,
                                            n.peerEpoch,
                                            n.state));

                            if (termPredicate(
                                            outofelection,
                                            new Vote(
                                                    n.version,
                                                    n.leader,
                                                    n.zxid,
                                                    n.electionEpoch,
                                                    n.peerEpoch,
                                                    n.state))
                                    && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                synchronized (this) {
                                    logicalclock.set(n.electionEpoch);
                                    self.setPeerState(
                                            (n.leader == self.getId())
                                                    ? ServerState.LEADING
                                                    : learningState());
                                }
                                Vote endVote =
                                        new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn(
                                    "Notification state unrecoginized: "
                                            + n.state
                                            + " (n.state), "
                                            + n.sid
                                            + " (n.sid)");
                            break;
                    }
                } else {
                    // 非法的票，打个日志
                    if (!validVoter(n.leader)) {
                        LOG.warn(
                                "Ignoring notification for non-cluster member sid {} from sid {}",
                                n.leader,
                                n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn(
                                "Ignoring notification for sid {} from non-quorum member sid {}",
                                n.leader,
                                n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if(self.jmxLeaderElectionBean != null){
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}",
                    manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }
}
