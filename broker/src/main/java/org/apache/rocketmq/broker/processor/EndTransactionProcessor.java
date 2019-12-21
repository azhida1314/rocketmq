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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * 处理事物消息提交或回滚的请求
 * EndTransaction processor: process commit and rollback message
 */
public class EndTransactionProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws
            RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final EndTransactionRequestHeader requestHeader =
                (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.info("Transaction request:{}", requestHeader);
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }

        if (requestHeader.getFromTransactionCheck()) {
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());

                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        } else {
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    return null;
                }

                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    break;
                }

                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message."
                                    + "RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                            requestHeader.toString(),
                            request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }
        //主要逻辑

        OperationResult result = new OperationResult();
        //是事务提交的类型
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
            //获取半消息
            result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);
            //找到了
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                //基本检查
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    //组装成新的  MessageExtBrokerInner
                    //替换消息的topic为真是的业务topic queueId为真实的业务Id
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
                    msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                    msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                    //存入commitLog中
                    RemotingCommand sendResult = sendFinalMessage(msgInner);
                    if (sendResult.getCode() == ResponseCode.SUCCESS) {
                        //删除事务半消息  不是真正的从halfTopic对应的队列中删除  是又向另一个opHalfTopic中加入了一条消息
                        //消息内容对应 halfTopic 的queueOffSet 并将 tags属性设置为'd'

                        //TODO？ 疑问  endMessageTransaction 这个方法中设置了         msgInner.setWaitStoreMsgOK(false); 那就不会同步关心刷盘的结果
                        //这样的化刷盘失败 并且消息还没有被消息 也会丢失吧？
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    }
                    return sendResult;
                }
                return res;
            }
            //是事务回滚的类型
        } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
            result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    //只是操作了删除 向另一个opHalfTopic中加入了一条消息 存储的halfTopic的逻辑偏移
                    this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                }
                return res;
            }
        }
        response.setCode(result.getResponseCode());
        response.setRemark(result.getResponseRemark());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt != null) {
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The producer group wrong");
                return response;
            }

            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The transaction state table offset wrong");
                return response;
            }

            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The commit log offset wrong");
                return response;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    /**
     * 组装真是的业务消息
     *
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        //异步刷盘相当于
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType =
                (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG
                        : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        //清空两个属性
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    /**
     * 保存消息
     *
     * @param msgInner
     * @return
     */
    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                    //下边的在事务消息变为真实消息发送中 应该不会有  设置的不关心刷盘结果
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("The message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("Service not available now.");
                    break;
                case OS_PAGECACHE_BUSY:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("OS page cache busy, please try another machine");
                    break;
                case UNKNOWN_ERROR:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }
            return response;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }
        return response;
    }
}
