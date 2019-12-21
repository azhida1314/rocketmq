/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * ：如果该消息回查的次数超过允许的最大回查次数，则该消息将被丢弃，即事务消息提交失败，不能被消费者消费，
     * 其做法，主要是每回查一次，在消息属性TRANSACTION_CHECK_TIMES中增1，默认最大回查次数为15次。
     *
     * @param msgExt              消息
     * @param transactionCheckMax 最大的回查次数
     * @return
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        //属性中获取回查次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * needSkip依据：如果事务消息超过文件的过期时间，默认72小时（具体请查看RocketMQ过期文件相关内容），则跳过该消息
     *
     * @param msgExt 消息
     * @return
     */
    private boolean needSkip(MessageExt msgExt) {
        //当前时间-消息的落盘时间
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        //大于文件的保留时间  默认72 小时
        if (valueOfCurrentMinusBorn
                > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
                * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                    msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                    putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                    putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.info(
                    "Send check message, the offset={} restored in queueOffset={} "
                            + "commitLogOffset={} "
                            + "newMsgId={} realMsgId={} topic={}",
                    offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                    msgExt.getTopic());
            return true;
        } else {
            log.error(
                    "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                            + "msgId: {}",
                    msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * 事物消息回查
     *
     * @param transactionTimeout  The minimum time of the transactional message to be checked firstly, one message only
     *                            exceed this time interval that can be checked.
     * @param transactionCheckMax The maximum number of times the message was checked, if exceed this value, this
     *                            message will be discarded.
     * @param listener            When the message is considered to be checked or discarded, the relative method of this class will
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
                      AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
            //根据主题名称，获取该主题下所有的消息队列。 当前半消息对列只有一个queue   0
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.info("Check topic={}, queues={}", topic, msgQueues);
            //循环遍历消息队列，从单个消息消费队列去获取消息。
            for (MessageQueue messageQueue : msgQueues) {
                //循环可以忽略 当前事务消息只有一个队列
                long startTime = System.currentTimeMillis();
                //通过 halfQueue 获取 opHalfQueue
                //因为在存入时是根据  halfQueue 构建的 opHalfQueue
                MessageQueue opQueue = getOpQueue(messageQueue);
                //获取操作队列的消费进度、
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                //待操作的消费队列的消费进度
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                //如果任意一小于0，忽略该消息队列，继续处理下一个队列。
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                            halfOffset, opOffset);
                    continue;
                }
                //调用fillOpRemoveMap主题填充 removeMap、doneOpOffset 数据结构，这里主要的目的是避免重复调用事务回查接口

                // doneOpOffset 可以确认的  已经是确定状态的消息
                List<Long> doneOpOffset = new ArrayList<>();

                //待确认的消息
                HashMap<Long, Long> removeMap = new HashMap<>();

                //填充两个集合
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                            messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                //获取空消息的次数
                int getMessageNullCount = 1;
                //当前处理 RMQ_SYS_TRANS_HALF_TOPIC#queueId的最新进度。
                long newOffset = halfOffset;
                //当前处理消息的队列偏移量，其主题依然为RMQ_SYS_TRANS_HALF_TOPIC
                long i = halfOffset;
                while (true) {
                    //RocketMQ为待检测主题RMQ_SYS_TRANS_HALF_TOPIC的每个队列，做事务状态回查，一次最多不超过60S，目前该值不可配置
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    //如果removeMap中包含当前处理的消息，则继续下一条  因为既然在removeMap中存在了  那证明这个半消息一定发送过commit或者rollback 不然不会到待确认
                    if (removeMap.containsKey(i)) {
                        log.info("Half offset {} has been committed/rolled back", i);
                        removeMap.remove(i);
                    } else {
                        //根据消息队列偏移量i从消费队列中获取消息。
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        //异常逻辑  可以先过
                        if (msgExt == null) {
                            //增加 获取空消息的次数 直接返回
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {//没有发现消息
                                log.info("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                        messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                        i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }
                        //判断该消息是否需要discard(吞没，丢弃，不处理)、或skip(跳过)
                        // 1、needDiscard 依据：如果该消息回查的次数超过允许的最大回查次数，则该消息将被丢弃，即事务消息提交失败，不能被消费者消费，其做法，主要是每回查一次，在消息属性TRANSACTION_CHECK_TIMES中增1，默认最大回查次数为15次。
                        //2、needSkip依据：如果事务消息超过文件的过期时间，默认72小时（具体请查看RocketMQ过期文件相关内容），则跳过该消息
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        //刚存进去的消息  不做回查
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.info("Fresh stored. the miss offset={}, check it later, store={}", i,
                                    new Date(msgExt.getStoreTimestamp()));
                            break;
                        }
                        //  valueOfCurrentMinusBorn ：该消息已存储的时间,等于系统当前时间减去消息存储的时间戳
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        // checkImmunityTime ：立即检测事务消息的时间，其设计的意义是，应用程序在发送事务消息后，事务不会马上提交，该时间就是假设事务消息发送成功后，应用程序事务提交的时间，在这段时间内，RocketMQ任务事务未提交，故不应该在这个时间段向应用程序发送回查请求。
                        //transactionTimeout：事务消息的超时时间，这个时间是从OP拉取的消息的最后一条消息的存储时间与check方法开始的时间，如果时间差超过了transactionTimeout，就算时间小于checkImmunityTime时间，也发送事务回查指令。
                        long checkImmunityTime = transactionTimeout;


                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);

                            //已经存储的时间  小于 立即回查的时间 （正常的还没提交过来的）
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                //如果 没有到时见  再把消息重新进行投递  当前的消费位点+1 下次虽然是同一个消息  但是却是被重新投递的
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.info("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                        checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        // 所有的 opHalf 消息
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        //是否要进行回查
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                                || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                                || (valueOfCurrentMinusBorn <= -1);

                        if (isNeedCheck) {
                            //需要检查  重新向HalfTopic 再次写入数据
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            //执行回查逻辑
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            //执行完这次后 再重新进行队列的填充
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                    messageQueue, pullResult);
                            continue;
                        }
                    }
                    //设置新的消费的逻辑偏移量  刚才是4的话  现在编程了5
                    newOffset = i + 1;
                    //新的逻辑偏移量
                    i++;
                }
                //这两个不想等 说明进行了回查
                if (newOffset != halfOffset) {
                    //更新消费位点为 newOffset
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                //计算信息的  OpOffset
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    //更新 OpOffset
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * 根据opHalfTopic的队列和halfTopic的队列填充 removeMap 和 doneOpOffset
     * <p>
     * doneOpOffset 已经处理完成的  不需要在进行处理的
     * <p>
     * removeMap 不确定的
     * <p>
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap      Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue        Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset     The current minimum offset of half message queue. //半消息队列的当前最小偏移量。
     * @param doneOpOffset   Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
                                       MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        //每次从  opQueue 的消费位点  向前查找 32 个消息 进行返回  或者不够则返回
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);

        //看正常流程
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            return pullResult;
        }

        //获得了消息的集合 opHalf
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }

        //循环 OpHalf
        for (MessageExt opMessageExt : opMsg) {
            //获取 halfTopic 的逻辑偏移位置
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.info("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {

                //如果 OpHalf 存的  Half 的逻辑偏移位置小于当前的 Half 的逻辑偏移位置  则当前消息已经被处理过了  放入到 doneOpOffset
                if (queueOffset < miniOffset) {
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    //否则  是待确认的  加入到  map中  key 是 halfTopic 的逻辑偏移位置 value 是 OpHalf 逻辑偏移位置
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap    Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt       Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
                                            MessageExt msgExt) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq     Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums   Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq     Target Message Queue
     * @param offset Offset in the message queue
     * @param nums   Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                    messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    /**
     * 获取半消息
     *
     * @param commitLogOffset
     * @return
     */
    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.info("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
