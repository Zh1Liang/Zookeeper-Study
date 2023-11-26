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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;
/**
 * This class has the control logic for the Follower.
 */
public class Follower extends Learner{

    private long lastQueued;
    // This is the same object as this.zk, but we cache the downcast op
    final FollowerZooKeeperServer fzk;

    Follower(QuorumPeer self,FollowerZooKeeperServer zk) {
        this.self = self;
        this.zk=zk;
        this.fzk = zk;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Follower ").append(sock);
        sb.append(" lastQueuedZxid:").append(lastQueued);
        sb.append(" pendingRevalidationCount:")
            .append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * the main method called by the follower to follow the leader
     *
     * @throws InterruptedException
     */
    //follower的核心方法
    void followLeader() throws InterruptedException {
        //当前时间
        self.end_fle = Time.currentElapsedTime();
        //选举耗时
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("FOLLOWING - LEADER ELECTION TOOK - {} {}", electionTimeTaken,
                QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;
        fzk.registerJMX(new FollowerBean(this, zk), self.jmxLocalPeerBean);
        try {
            self.setZabState(QuorumPeer.ZabState.DISCOVERY);
            QuorumServer leaderServer = findLeader();
            try {
                //通过BIO连接到Leader
                connectToLeader(leaderServer.addr, leaderServer.hostname);

                //注册到Leader
                long newEpochZxid = registerWithLeader(Leader.FOLLOWERINFO);

                if (self.isReconfigStateChange())
                   throw new Exception("learned about role change");
                //check to see if the leader zxid is lower than ours
                //this should never happen but is just a safety check
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch " + ZxidUtils.zxidToString(newEpochZxid)
                            + " is less than our accepted epoch " + ZxidUtils.zxidToString(self.getAcceptedEpoch()));
                    throw new IOException("Error: Epoch of leader is lower");
                }
                self.setZabState(QuorumPeer.ZabState.SYNCHRONIZATION);

                //数据同步
                syncWithLeader(newEpochZxid);
                self.setZabState(QuorumPeer.ZabState.BROADCAST);
                QuorumPacket qp = new QuorumPacket();

                //死循环，不断去从leader拉数据
                while (this.isRunning()) {
                    //读数据
                    //Leader 发送请求给 Follower 后，Follower 通过此方法将请求数据读到 QuorumPacket 对象中
                    readPacket(qp);
                    //处理数据
                    //处理Leader 发来的请求数据，也就是 QuorumPacket 对象
                    processPacket(qp);
                }
            } catch (Exception e) {
                LOG.warn("Exception when following the leader", e);
                closeSocket();
    
                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            zk.unregisterJMX((Learner)this);
        }
    }

    /**
     * Examine the packet received in qp and dispatch based on its contents.
     * @param qp
     * @throws IOException
     */
    protected void processPacket(QuorumPacket qp) throws Exception{
        switch (qp.getType()) {
        // 心跳请求，刷新Session
        case Leader.PING:            
            ping(qp);            
            break;
        // Leader发来的propose请求
        case Leader.PROPOSAL:
            TxnHeader hdr = new TxnHeader();
            Record txn = SerializeUtils.deserializeTxn(qp.getData(), hdr);
            if (hdr.getZxid() != lastQueued + 1) {
                LOG.warn("Got zxid 0x"
                        + Long.toHexString(hdr.getZxid())
                        + " expected 0x"
                        + Long.toHexString(lastQueued + 1));
            }
            lastQueued = hdr.getZxid();
            
            if (hdr.getType() == OpCode.reconfig){
               SetDataTxn setDataTxn = (SetDataTxn) txn;       
               QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));
               self.setLastSeenQuorumVerifier(qv, true);                               
            }
            //处理proposal
            fzk.logRequest(hdr, txn);
            break;
        // Leader发来的commit请求
        case Leader.COMMIT:
            fzk.commit(qp.getZxid());
            break;
            
        case Leader.COMMITANDACTIVATE:
           // get the new configuration from the request
           Request request = fzk.pendingTxns.element();
           SetDataTxn setDataTxn = (SetDataTxn) request.getTxn();                                                                                                      
           QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));                                
 
           // get new designated leader from (current) leader's message
           ByteBuffer buffer = ByteBuffer.wrap(qp.getData());    
           long suggestedLeaderId = buffer.getLong();
            boolean majorChange =
                   self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
           // commit (writes the new config to ZK tree (/zookeeper/config)
           fzk.commit(qp.getZxid());
            if (majorChange) {
               throw new Exception("changes proposed in reconfig");
           }
           break;
        case Leader.UPTODATE:
            LOG.error("Received an UPTODATE message after Follower started");
            break;
        case Leader.REVALIDATE:
            revalidate(qp);
            break;
        case Leader.SYNC:
            fzk.sync();
            break;
        default:
            LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
            break;
        }
    }

    /**
     * The zxid of the last operation seen
     * @return zxid
     */
    public long getZxid() {
        try {
            synchronized (fzk) {
                return fzk.getZxid();
            }
        } catch (NullPointerException e) {
            LOG.warn("error getting zxid", e);
        }
        return -1;
    }
    
    /**
     * The zxid of the last operation queued
     * @return zxid
     */
    protected long getLastQueued() {
        return lastQueued;
    }

    @Override
    public void shutdown() {    
        LOG.info("shutdown called", new Exception("shutdown Follower"));
        super.shutdown();
    }
}
