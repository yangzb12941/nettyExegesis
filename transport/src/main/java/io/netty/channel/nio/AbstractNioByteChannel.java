/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.FileRegion;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.util.internal.StringUtil;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on bytes.
 */
public abstract class AbstractNioByteChannel extends AbstractNioChannel {

    private static final String EXPECTED_TYPES =
            " (expected: " + StringUtil.simpleClassName(ByteBuf.class) + ", " +
            StringUtil.simpleClassName(FileRegion.class) + ')';

    private Runnable flushTask;

    /**
     * Create a new instance
     *
     * @param parent            the parent {@link Channel} by which this instance was created. May be {@code null}
     * @param ch                the underlying {@link SelectableChannel} on which it operates
     */
    protected AbstractNioByteChannel(Channel parent, SelectableChannel ch) {
        super(parent, ch, SelectionKey.OP_READ);
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        return new NioByteUnsafe();
    }

    protected class NioByteUnsafe extends AbstractNioUnsafe {
        private RecvByteBufAllocator.Handle allocHandle;

        private void closeOnRead(ChannelPipeline pipeline) {
            SelectionKey key = selectionKey();
            setInputShutdown();
            if (isOpen()) {
                if (Boolean.TRUE.equals(config().getOption(ChannelOption.ALLOW_HALF_CLOSURE))) {
                    key.interestOps(key.interestOps() & ~readInterestOp);
                    pipeline.fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
                } else {
                    close(voidPromise());
                }
            }
        }

        private void handleReadException(ChannelPipeline pipeline,
                                         ByteBuf byteBuf, Throwable cause, boolean close) {
            if (byteBuf != null) {
                if (byteBuf.isReadable()) {
                    setReadPending(false);
                    pipeline.fireChannelRead(byteBuf);
                } else {
                    byteBuf.release();
                }
            }
            pipeline.fireChannelReadComplete();
            pipeline.fireExceptionCaught(cause);
            if (close || cause instanceof IOException) {
                closeOnRead(pipeline);
            }
        }

        @Override
        public final void read() {
            final ChannelConfig config = config();
            if (!config.isAutoRead() && !isReadPending()) {
                // ChannelConfig.setAutoRead(false) was called in the meantime
                removeReadOp();
                return;
            }

            final ChannelPipeline pipeline = pipeline();
            //这一步是获取一个 ByteBuf 的内存分配器，用于分配ByteBuf： allocator 就 是 PooledByteBufAllocator
            final ByteBufAllocator allocator = config.getAllocator();
            //获取消息读取次数，默认最多读取16次
            final int maxMessagesPerRead = config.getMaxMessagesPerRead();
            //Handle 是 对 RecvByteBufAllocator 进 行 实 际 操 作 的 对 象
            RecvByteBufAllocator.Handle allocHandle = this.allocHandle;
            if (allocHandle == null) {
                this.allocHandle = allocHandle = config.getRecvByteBufAllocator().newHandle();
            }

            ByteBuf byteBuf = null;
            int messages = 0;
            boolean close = false;
            try {
                int totalReadAmount = 0;
                boolean readPendingReset = false;
                do {
                    //这里传入了之前创建的allocate对象，也就是PooledByteBufAllocator，这里会调用
                    //DefaultMaxMessagesRecvByteBufAllocator类的allocate()方法。
                    byteBuf = allocHandle.allocate(allocator);
                    int writable = byteBuf.writableBytes();
                    //将Channel中的数据读取到刚分配的ByteBuf中，并返回读取的字节数，这里会调用
                    //NioSocketChannel的doReadBytes()方法。
                    int localReadAmount = doReadBytes(byteBuf);
                    //localReadAmount代表最后读取的字节数，赋值为写入ByteBuf的字节数
                    //如果最后一次读取数据为0，说明已经将Channel中的数据全部读取完毕，将
                    //新 创 建 的 ByteBuf 释 放 循 环 利 用 ， 并 跳 出 循 环 。
                    if (localReadAmount <= 0) {
                        // not was read release the buffer
                        byteBuf.release();
                        byteBuf = null;
                        close = localReadAmount < 0;
                        if (close) {
                            // There is nothing left to read as we received an EOF.
                            setReadPending(false);
                        }
                        break;
                    }
                    if (!readPendingReset) {
                        readPendingReset = true;
                        setReadPending(false);
                    }
                    pipeline.fireChannelRead(byteBuf);
                    byteBuf = null;
                    //totalReadAmount表示总共读取的字节数，这里将写入的字节数追加。
                    if (totalReadAmount >= Integer.MAX_VALUE - localReadAmount) {
                        // Avoid overflow.
                        totalReadAmount = Integer.MAX_VALUE;
                        break;
                    }

                    totalReadAmount += localReadAmount;

                    // stop reading
                    if (!config.isAutoRead()) {
                        break;
                    }

                    if (localReadAmount < writable) {
                        // Read less than what the buffer can hold,
                        // which might mean we drained the recv buffer completely.
                        break;
                    }
                    //++ messages 增加消息的读取次数，因为最
                    //多循环16次，所以当消息次数增加到16时会结束循环。
                } while (++ messages < maxMessagesPerRead);
                //读取完毕之后，传递ChannelRead事件。
                //至此，小伙伴们应该有个疑问，如果一次读取不完，就传递
                //ChannelRead事件，那么Server接收到的数据有可能就是不完整的，其
                //实关于这点，Netty也做了相应的处理，我们会在之后的章节详细剖析Netty 的 半 包 处 理 机 制 。
                pipeline.fireChannelReadComplete();
                //我们知道第一次分配ByteBuf的初始容量是1024字节，但是初始容
                //量不一定满足所有的业务场景。Netty中，将每次读取数据的字节数进
                //行记录，然后之后次分配ByteBuf的时候，容量会尽可能地符合业务场
                //景所需要的大小，具体实现方式就是在 record() 这一步体现的。
                allocHandle.record(totalReadAmount);

                if (close) {
                    closeOnRead(pipeline);
                    close = false;
                }
            } catch (Throwable t) {
                handleReadException(pipeline, byteBuf, t, close);
            } finally {
                // Check if there is a readPending which was not processed yet.
                // This could be for two reasons:
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelRead(...) method
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelReadComplete(...) method
                //
                // See https://github.com/netty/netty/issues/2254
                if (!config.isAutoRead() && !isReadPending()) {
                    removeReadOp();
                }
            }
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        int writeSpinCount = -1;

        boolean setOpWrite = false;
        for (;;) {
            //每次获取当前节点
            Object msg = in.current();//这一步是获取flushedEntry指向的Entry中的msg
            if (msg == null) {
                // Wrote all messages.
                //如 果 msg 为 null ， 说 明 没 有 可 以 刷 新 的 Entry ， 则 调 用
                //clearOpWrite()方法清除写标识。
                clearOpWrite();
                // Directly return here so incompleteWrite(...) is not called.
                return;
            }

            if (msg instanceof ByteBuf) {
                //转化成ByteBuf
                ByteBuf buf = (ByteBuf) msg;
                //如果没有可写的值
                int readableBytes = buf.readableBytes();
                if (readableBytes == 0) {
                    //移除
                    in.remove();
                    continue;
                }
                //标识刷新操作是 否 执 行 完 成 ， 默 认 值 false 代 表 执 行 到 这 里 没 有 执 行 完
                boolean done = false;
                long flushedAmount = 0;
                if (writeSpinCount == -1) {
                    //获取写操作的迭代次数，默认是16次
                    writeSpinCount = config().getWriteSpinCount();
                }
                for (int i = writeSpinCount - 1; i >= 0; i --) {
                    //将Buffer写入Socket
                    //localFlushedAmount 代表向JDK底层写了多少字节
                    //将Buffer的内容写入Channel，并返回写的字节数，这
                    //里会调用NioSocketChannel的doWriteBytes
                    int localFlushedAmount = doWriteBytes(buf);
                    //如果一个字节都没有则直接Break
                    //如果一个字节都没写，说明现在Channel可能不可写，将
                    //setOpWrite设置为true，用于标识写操作位，并退出循环。
                    if (localFlushedAmount == 0) {
                        setOpWrite = true;
                        break;
                    }
                    //统计总共写了多少字节
                    //如果已经写出字节，则通过flushedAmount+=localFlushedAmount累加写出的字节
                    //数，然后根据Buffer是否没有可读字节数判断Buffer的数据是否已经写
                    //完，如果写完，将done设置为true，说明写操作完成，并退出循环。因
                    //为有时候不一定一次就能将ByteBuf所有的字节写完，所以会继续通过
                    //循环进行写出，直到循环16次。
                    flushedAmount += localFlushedAmount;
                    //如果Buffer全部写入JDK底层
                    if (!buf.isReadable()) {
                        //标记全部写入
                        done = true;
                        break;
                    }
                }

                in.progress(flushedAmount);

                if (done) {
                    //移除当前对象
                    in.remove();
                } else {
                    // Break the loop and so incompleteWrite(...) is called.
                    break;
                }
            } else if (msg instanceof FileRegion) {
                FileRegion region = (FileRegion) msg;
                boolean done = region.transfered() >= region.count();

                if (!done) {
                    long flushedAmount = 0;
                    if (writeSpinCount == -1) {
                        writeSpinCount = config().getWriteSpinCount();
                    }

                    for (int i = writeSpinCount - 1; i >= 0; i--) {
                        long localFlushedAmount = doWriteFileRegion(region);
                        if (localFlushedAmount == 0) {
                            setOpWrite = true;
                            break;
                        }

                        flushedAmount += localFlushedAmount;
                        if (region.transfered() >= region.count()) {
                            done = true;
                            break;
                        }
                    }

                    in.progress(flushedAmount);
                }

                if (done) {
                    in.remove();
                } else {
                    // Break the loop and so incompleteWrite(...) is called.
                    break;
                }
            } else {
                // Should not reach here.
                throw new Error();
            }
        }
        incompleteWrite(setOpWrite);
    }

    @Override
    protected final Object filterOutboundMessage(Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            //是堆外内存，直接返回
            if (buf.isDirect()) {
                return msg;
            }
            return newDirectBuffer(buf);
        }

        if (msg instanceof FileRegion) {
            return msg;
        }

        throw new UnsupportedOperationException(
                "unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
    }

    protected final void incompleteWrite(boolean setOpWrite) {
        // Did not write completely.
        if (setOpWrite) {
            setOpWrite();
        } else {
            // Schedule flush again later so other tasks can be picked up in the meantime
            Runnable flushTask = this.flushTask;
            if (flushTask == null) {
                flushTask = this.flushTask = new Runnable() {
                    @Override
                    public void run() {
                        flush();
                    }
                };
            }
            eventLoop().execute(flushTask);
        }
    }

    /**
     * Write a {@link FileRegion}
     *
     * @param region        the {@link FileRegion} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract long doWriteFileRegion(FileRegion region) throws Exception;

    /**
     * Read bytes into the given {@link ByteBuf} and return the amount.
     */
    protected abstract int doReadBytes(ByteBuf buf) throws Exception;

    /**
     * Write bytes form the given {@link ByteBuf} to the underlying {@link java.nio.channels.Channel}.
     * @param buf           the {@link ByteBuf} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract int doWriteBytes(ByteBuf buf) throws Exception;

    protected final void setOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) == 0) {
            key.interestOps(interestOps | SelectionKey.OP_WRITE);
        }
    }

    protected final void clearOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) != 0) {
            key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
        }
    }
}
