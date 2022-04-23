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
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * A decoder that splits the received {@link ByteBuf}s on line endings.
 * <p>
 * Both {@code "\n"} and {@code "\r\n"} are handled.
 * For a more general delimiter-based decoder, see {@link DelimiterBasedFrameDecoder}.
 *
 * LineBasedFrameDecoder是回车换行解码器，如果用户发送的消息
 * 以回车换行符（以\r\n或者直接以\n结尾）作为消息结束的标识，则可
 * 以直接使用Netty的LineBasedFrameDecoder对消息进行解码，只需要在
 * 初始化Netty服务端或者客户端时将LineBasedFrameDecoder正确地添加
 * 到ChannelPipeline中即可，不需要自己重新实现一套换行解码器。
 *
 * LineBasedFrameDecoder的工作原理是它依次遍历ByteBuf中的可读
 * 字节，判断是否有“\n”或者“\r\n”，如果有，就以此位置为结束位
 * 置，从可读索引到结束位置区间的字节就组成了一行。它是以换行符为
 * 结束标志的解码器，支持携带结束符或者不携带结束符两种解码方式，
 * 同时支持配置单行的最大长度。如果连续读取到最大长度后仍然没有发
 * 现换行符，就会抛出异常，同时忽略之前读到的异常码流，防止由于数
 * 据报没有携带换行符导致接收到的ByteBuf无限制积压，引起系统内存
 * 溢出。它的使用效果如下。
 *
 * 通常情况下，LineBasedFrameDecoder会和StringDecoder配合使
 * 用，组合成按行切换的文本解码器，对于文本类协议的解析，文本换行
 * 解码器非常实用，例如对HTTP消息头的解析、FTP消息的解析等。
 *
 * pipeline.addLast(new LineBasedFrameDecode(1024));
 * pipeline.addLast(new StringDecoder());
 *
 */
public class LineBasedFrameDecoder extends ByteToMessageDecoder {

    /** Maximum length of a frame we're willing to decode.  */
    //数据包的最大长度，超过该长度会进行丢弃模式
    private final int maxLength;
    /** Whether or not to throw an exception as soon as we exceed maxLength. */
    //超过最大长度是否要抛出异常
    private final boolean failFast;
    //最终解析的数据包是否带有换行符
    private final boolean stripDelimiter;

    /** True if we're discarding input because we're already over maxLength.  */
    //为true说明当前解码过程为丢弃模式
    private boolean discarding;
    //丢弃了多少字节
    private int discardedBytes;

    /** Last scan position. */
    private int offset;

    /**
     * Creates a new decoder.
     * @param maxLength  the maximum length of the decoded frame.
     *                   A {@link TooLongFrameException} is thrown if
     *                   the length of the frame exceeds this value.
     */
    public LineBasedFrameDecoder(final int maxLength) {
        this(maxLength, true, false);
    }

    /**
     * Creates a new decoder.
     * @param maxLength  the maximum length of the decoded frame.
     *                   A {@link TooLongFrameException} is thrown if
     *                   the length of the frame exceeds this value.
     * @param stripDelimiter  whether the decoded frame should strip out the
     *                        delimiter or not
     * @param failFast  If <tt>true</tt>, a {@link TooLongFrameException} is
     *                  thrown as soon as the decoder notices the length of the
     *                  frame will exceed <tt>maxFrameLength</tt> regardless of
     *                  whether the entire frame has been read.
     *                  If <tt>false</tt>, a {@link TooLongFrameException} is
     *                  thrown after the entire frame that exceeds
     *                  <tt>maxFrameLength</tt> has been read.
     */
    public LineBasedFrameDecoder(final int maxLength, final boolean stripDelimiter, final boolean failFast) {
        this.maxLength = maxLength;
        this.failFast = failFast;
        this.stripDelimiter = stripDelimiter;
    }

    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Object decoded = decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }
    }

    /**
     * Create a frame out of the {@link ByteBuf} and return it.
     *
     * @param   ctx             the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param   buffer          the {@link ByteBuf} from which to read data
     * @return  frame           the {@link ByteBuf} which represent the frame or {@code null} if no frame could
     *                          be created.
     */
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        //找这行的结尾：是寻找当前行的结尾的索引值，也就是\r\n或者是\n
        //[][]    ...    [\n][][]   ...   [\r][\n][]...
        //↑                ↑                ↑
        //readIndex       eol              eol
        final int eol = findEndOfLine(buffer);
        if (!discarding) {
            //首先获得换行符到可读字节之间的长度，然后获取换行符的长度，
            //如果是\n结尾，则长度为1；如果是\r结尾，则长度为2。if(length >
            //maxLength) 代 表 如 果 长 度 超 过 最 大 长 度 ， 则 直 接 通 过
            //readerIndex(eol+delimLength)这种方式将读指针指向换行符之后的字
            //节，说明换行符之前的字节需要完全丢弃，如下图所示。
            //     ↓ 超过最大值进行丢弃  ↓
            //... [][][]...     [\r][\n][][][][] ...
            //                   ↑       ↑
            //                  eol  readIndex
            if (eol >= 0) {
                final ByteBuf frame;
                //计算从换行符到可读字节之间的长度
                final int length = eol - buffer.readerIndex();
                //获得分隔符长度，如果是\r\n结尾，分隔符长度为2
                final int delimLength = buffer.getByte(eol) == '\r'? 2 : 1;
                //如果长度大于最大长度
                if (length > maxLength) {
                    //指向换行符之后的可读字节(这段数据完全丢弃)
                    buffer.readerIndex(eol + delimLength);
                    //传播异常事件
                    fail(ctx, length);
                    return null;
                }
                //如果这次解析的数据是有效的，
                //分隔符是否算在完整数据包里
                //true为丢弃分隔符
                if (stripDelimiter) {
                    //截取有效长度
                    frame = buffer.readSlice(length);
                    //跳过分隔符的字节
                    buffer.skipBytes(delimLength);
                } else {
                    frame = buffer.readSlice(length + delimLength);
                }
                return frame.retain();
            } else {
                //如果没找到分隔符(非丢弃模式)
                //可读字节长度
                final int length = buffer.readableBytes();
                //如果超过能解析的最大长度
                //     ↓ 未找到换行符且超过最大值，全部丢弃并进入丢弃模式  ↓
                //... [][][]            ...             [][][][][][] ...
                //     ↑                                          ↑
                //  readIndex                                  writeIndex
                //这里buffer.readerIndex(buffer.writerIndex())直接将读指针移
                //动到写指针，并且将discarding设置为true，就是丢弃模式。如果可读
                //字节没有超过最大长度，则返回null，表示什么都没解析出来，等着下
                //次解析。
                if (length > maxLength) {
                    //将当前长度标记为可丢弃的
                    discardedBytes = length;
                    //直接将读指针移动到写指针
                    buffer.readerIndex(buffer.writerIndex());
                    //标记为丢弃模式
                    discarding = true;
                    offset = 0;
                    //超出最大长度抛出异常
                    if (failFast) {
                        fail(ctx, "over " + discardedBytes);
                    }
                }
                //没有超过，则直接返回
                return null;
            }
        } else {
            //丢弃模式
            //     ↓ 超过最大值进行丢弃  ↓
            //... [][][]...     [\r][\n][][][][] ...
            //                   ↑       ↑
            //                  eol  readIndex
            //final int length=discardedBytes+eol-buffer.readerIndex()获
            //得丢弃的字节总数，也就是之前丢弃的字节数+现在需要丢弃的字节数。
            //然后计算换行符的长度，如果是\n则是1，如果是\r\n就是2
            if (eol >= 0) {
                //找到分隔符
                //当前丢弃的字节(前面已经丢弃的+现在丢弃的位置-写指针)
                final int length = discardedBytes + eol - buffer.readerIndex();
                //当前换行符长度为多少
                final int delimLength = buffer.getByte(eol) == '\r'? 2 : 1;
                //读指针直接移到换行符+换行符的长度
                buffer.readerIndex(eol + delimLength);
                //当前丢弃的字节为0
                discardedBytes = 0;
                //设置为未丢弃模式
                discarding = false;
                //丢弃完字节后触发异常
                if (!failFast) {
                    fail(ctx, length);
                }
            } else {
                //累计已丢弃的字节个数+当前可读的长度
                discardedBytes += buffer.readableBytes();
                //移动
                buffer.readerIndex(buffer.writerIndex());
            }
            return null;
        }
    }

    private void fail(final ChannelHandlerContext ctx, int length) {
        fail(ctx, String.valueOf(length));
    }

    private void fail(final ChannelHandlerContext ctx, String length) {
        ctx.fireExceptionCaught(
                new TooLongFrameException(
                        "frame length (" + length + ") exceeds the allowed maximum (" + maxLength + ')'));
    }

    /**
     * Returns the index in the buffer of the end of line found.
     * Returns -1 if no end of line was found in the buffer.
     */
    private int findEndOfLine(final ByteBuf buffer) {
        int totalLength = buffer.readableBytes();
        //如果找到了，并且前面的字符是\r,则指向\r字节
        //如果找到了，并且前面是\r，则返回\r的索引，否则返回\n的索引
        int i = buffer.forEachByte(buffer.readerIndex() + offset, totalLength - offset, ByteBufProcessor.FIND_LF);
        if (i >= 0) {
            offset = 0;
            if (i > 0 && buffer.getByte(i - 1) == '\r') {
                i--;
            }
        } else {
            offset = totalLength;
        }
        return i;
    }
}
