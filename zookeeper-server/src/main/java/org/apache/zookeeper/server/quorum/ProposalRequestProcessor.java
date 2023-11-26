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

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG =
        LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;

    RequestProcessor nextProcessor;

    SyncRequestProcessor syncProcessor;

    public ProposalRequestProcessor(LeaderZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        //这里也有一个责任链
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
    }

    /**
     * initialize this processor
     */
    public void initialize() {
        syncProcessor.start();
    }

    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");


        /* In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         */
        // 如果是Follower/Observer则走if分支，Leader直接走else
        //request应该是Pre处理的，可以看一下，因为被转换成了其他类型
        if (request instanceof LearnerSyncRequest){
            zks.getLeader().processSync((LearnerSyncRequest)request);
        } else {
            //1、调用 CommitProcessor.processRequest(request) ，最终的目的是负责将数据写到内存的(第二阶段就是写入内存，可读)
            // 写到内存后就可以对外读取了（先写入本地事务日志文件，当半数ack后就可以提交到内存）
            //一开始的时候 CommitProcessor 线程任务会阻塞住，也就是 wait() 住
            //等第二第三步将提议发给 Follower 且得到过半的反馈ack后会进行 notifyAll() 唤醒正在阻塞的 CommitProcessor 线程
            nextProcessor.processRequest(request);
            //2、向Follower发起提议,也就是通知各个 Follower 节点要写入这个数据到事物日志
            if (request.getHdr() != null) {
                // We need to sync and get consensus on any transactions
                try {
                    //构造提案并异步发送，所以这里不会阻塞
                    zks.getLeader().propose(request);
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                // 3、调用SyncRequestProcessor的processRequest,
                // 将数据写入本地事物日志文件，且返回 ack，也就是给自己记一票(Leader的逻辑)
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
