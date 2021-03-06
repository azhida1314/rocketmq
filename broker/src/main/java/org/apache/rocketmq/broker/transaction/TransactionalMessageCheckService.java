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
package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 执行事物消息的回查逻辑
 */
public class TransactionalMessageCheckService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public TransactionalMessageCheckService(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 启动线程
     */
    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            super.start();
            this.brokerController.getTransactionalMessageService().open();
        }
    }

    @Override
    public void shutdown(boolean interrupt) {
        if (started.compareAndSet(true, false)) {
            super.shutdown(interrupt);
            this.brokerController.getTransactionalMessageService().close();
            this.brokerController.getTransactionalMessageCheckListener().shutDown();
        }
    }

    @Override
    public String getServiceName() {
        return TransactionalMessageCheckService.class.getSimpleName();
    }

    /**
     * 线程执行的方法
     */
    @Override
    public void run() {
        log.info("Start transaction check service thread!");
        //事务执行回查任务的间隔 默认60秒
        long checkInterval = brokerController.getBrokerConfig().getTransactionCheckInterval();
        while (!this.isStopped()) {
            //执行
            this.waitForRunning(checkInterval);
        }
        log.info("End transaction check service thread!");
    }

    /**
     * 时间到了执行  执行回查逻辑
     */
    @Override
    protected void onWaitEnd() {
        // 事物的过期时间 一个消息的存储时间 + 该值 大于系统当前时间，才对该消息执行事务状态会查。

        //事务消息可以回查的间隔
        long timeout = brokerController.getBrokerConfig().getTransactionTimeOut();
        //事物回查最大次数  如果超过检测次数，消息会默认为丢弃，即rollback消息。
        int checkMax = brokerController.getBrokerConfig().getTransactionCheckMax();
        long begin = System.currentTimeMillis();
        log.info("Begin to check prepare message, begin time:{}", begin);
        this.brokerController.getTransactionalMessageService().check(timeout, checkMax, this.brokerController.getTransactionalMessageCheckListener());
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }

}
