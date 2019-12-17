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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.*;

/**
 * 创建映射文件的线程
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 创建映射文件的超时时间 5S
     */
    private static int waitTimeOut = 1000 * 5;
    /**
     * 用来保存所有当前待处理的分配请求
     * 其中  key 是filePath value 是分配请求
     * 如果分配请求被成功处理  即获取到映射文件  则从此队列中删除
     */
    private ConcurrentMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<String, AllocateRequest>();
    /**
     * 分配请求的队列
     * 是一个优先级队列 从这个对队列中获取请求  根据请求创建映射文件
     * 每次都提交两个请求 只需要等待最小的文件请求创建成功，  请求实现了 Comparable接口  可以比较
     */
    private PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<AllocateRequest>();
    /**
     * 标记是否发生异常
     */
    private volatile boolean hasException = false;
    /**
     * 消息存储的入口服务
     */
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 根据请求创建 映射文件
     *
     * @param nextFilePath     要创建的映射文件
     * @param nextNextFilePath 要创建的映射文件的下一个映射文件
     * @param fileSize         映射文件的大小
     * @return
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        /**
         * 默认处理的请求数为2
         */
        int canSubmitRequests = 2;
        //是否开启池化
        //仅当 transientStorePoolEnable 为true FlushDiskType 为 ASYNC_FLUSH 并且broker为主节点时  才启用 transientStorePool
        //同时启用 快速失败策略时，计算 transientStorePool 中剩余的 buffer 数量 减去 requestQueue 中待分配的请求的数量后，剩余的 buffer 数量
        //如果剩余的 buffer 数量小于等于 0 则快速失败
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            //
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                    //如果为从节点 即使没有 buffer 也不快速失败
                    && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                //当前可处理的请求数  = 池中的byteBuffer数-请求队列的数
                canSubmitRequests = this.messageStore.getTransientStorePool().remainBufferNumbs() - this.requestQueue.size();
            }
        }
        //组装请求
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        //向 请求队列中 放入路径和请求
        // 如果已经添加过 不会走 nextPutOK = true的逻辑 判断可以处理的请求数
        //因为之前已经计算过 并做了预留
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        if (nextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                //没有buffer可用 不能处理当前请求 直接从请求列表中删除
                this.requestTable.remove(nextFilePath);
                return null;
            }
            //入优先级阻塞队列  在一个永真循环中 处理提交到队列中的数据   org.apache.rocketmq.store.AllocateMappedFileService.run
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            //提交后 可处理的请求数减1
            canSubmitRequests--;
        }
        //要创建文件的下一个文件  和第一个文件的流程一样
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
                //不能处理直接从请求列表中删除
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }
        //是否有异常
        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }
        //获取请求  从请求中拿创建好的映射文件
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                //阻塞进行等待
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    //获取完第一个  把请求从 requestTable 移除
                    this.requestTable.remove(nextFilePath);
                    //返回 本次创建的内存映射文件
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }
        //返回null
        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();

        try {
            this.thread.join(this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    /**
     * 服务启动后一直在运行
     */
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 创建 映射文件
     * 此方法只有被外部中断才会返回false
     * Only interrupted by the external thread, will return false
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            //检索并删除此队列的首节点 必要时等待  直到有元素可用
            req = this.requestQueue.take();
            //从请求队列中根据路径获取一遍请求
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            //如果请求队列中没有 则不处理
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize());
                return true;
            }
            //两个请求不相等也不处理
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }
            //请求还没有执行  执行过后这个字段会有值
            if (req.getMappedFile() == null) {
                //开始
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                //仅当transientStorePoolEnable为true并且FlushDiskType为ASYNC_FLUSH时，才启用commitLog存储池
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        //通过哦 java 的 spi机制 获取 咩有发现有这个实现类
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        //一般都是走默认逻辑
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    //非池化的创建
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }
                //耗时
                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                //计算创建映射文件耗时
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + eclipseTime + " queue size " + queueSize
                            + " " + req.getFilePath() + " " + req.getFileSize());
                }
                //预写 mappedFile  当前映射文件的大小大于等于 CommitLog 文件的大小 broker开启了内存映射文件预热
                // pre write mappedFile
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                        .getMapedFileSizeCommitLog() && this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    //执行文件预热
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                            this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }
                //放入到请求中  会被外部方法获取
                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            //标记发生异常
            this.hasException = true;
            //被中断结束服务线程
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            //标记发生异常 但并不会标记结束服务线程
            this.hasException = true;
            if (null != req) {
                //重新加入队列再试
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                //通知等待的任务执行
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    /**
     * 内部类  代表分配请求 实现了 Comparable 接口
     * 用户自定义分配请求在请求队列的优先级
     */
    static class AllocateRequest implements Comparable<AllocateRequest> {
        /**
         * 文件全路径
         * Full file path
         */
        private String filePath;
        /**
         * 文件大小
         */
        private int fileSize;
        /**
         * 用于实现分配映射文件的等待通知模型
         * 初始值为1 0代表完成
         */
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        /**
         * 根据路径和映射文件创建的映射文件
         * 优先级高的那一个 等待第一个创建完即可返回
         */
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
