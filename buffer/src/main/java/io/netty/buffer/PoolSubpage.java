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

package io.netty.buffer;

/**
 * 在很多场景下，为缓冲区分配8KB的内存也是一种浪费，比如只需
 * 要分配2KB的缓冲区，如果使用8KB会造成6KB的浪费。这种情况下，
 * Netty又会将Page切分成多个SubPage，每个SubPage大小要根据分配的
 * 缓冲区大小而定，比如要分配2KB的内存，就会将一个Page切分成4个
 * SubPage，每个SubPage的大小为2KB。
 * @param <T>
 */
final class PoolSubpage<T> implements PoolSubpageMetric {

    final PoolChunk<T> chunk;//代表其子页属于哪个Chunk
    private final int memoryMapIdx;
    private final int runOffset;
    private final int pageSize;
    private final long[] bitmap;//bitmap用于记录子页的内存分配情况

    //prev和next代表子页是按照双向链表进行关联的，分别指向上一
    //个节点和下一个节点
    PoolSubpage<T> prev;
    PoolSubpage<T> next;

    boolean doNotDestroy;
    //elemSize属性代表的是子页是按照多大内存进行
    //划分的，如果按照1KB划分，则可以划分出8个子页
    int elemSize;
    private int maxNumElems;
    private int bitmapLength;
    private int nextAvail;
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Special constructor that creates a linked list head */
    PoolSubpage(int pageSize) {
        chunk = null;
        memoryMapIdx = -1;
        runOffset = -1;
        elemSize = -1;
        this.pageSize = pageSize;
        bitmap = null;
    }

    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int memoryMapIdx, int runOffset, int pageSize, int elemSize) {
        this.chunk = chunk;
        this.memoryMapIdx = memoryMapIdx;
        this.runOffset = runOffset;
        this.pageSize = pageSize;
        //这里重点关注bitmap属性，这是一个long类型的数组，初始大小为
        //8，这只是初始化的大小，真正的大小要根据Page切分成多少块确定
        bitmap = new long[pageSize >>> 10]; // pageSize / 16 / 64
        init(head, elemSize);
    }

    void init(PoolSubpage<T> head, int elemSize) {
        doNotDestroy = true;
        //表示保存当前分配的缓冲区大小，因为以16 字 节 为 例 ， 所 以 这 里 是 16
        this.elemSize = elemSize;
        if (elemSize != 0) {
            //初 始 化 了 两 个 属 性
            //maxNumElems和numAvail，值都为pageSize/elemSize，表示一个Page大
            //小除以分配的缓冲区大小，也就是表示当前Page被划分了多少份。
            //numAvail表示剩余可用的块数
            maxNumElems = numAvail = pageSize / elemSize;
            nextAvail = 0;
            //bitmapLength表示bitmap的实际大小，已经分
            //析过，bitmap初始化大小为8，但实际上并不一定需要8个元素，元素个
            //数要根据Page切分的子块而定，这里的大小是所切分的子块数除以64。
            bitmapLength = maxNumElems >>> 6;

            //判断maxNumElems也就是当前配置所切分的子块是不是64的倍数，如果不
            //是，则bitmapLength加1，最后通过循环，将对应位置的子块标记为0。
            if ((maxNumElems & 63) != 0) {
                bitmapLength ++;
            }

            //这里详细分析一下bitmap，它是个long类型的数组，long数组中的
            //每一个值，也都是long类型的数字，其中每一个比特位都标记着Page中
            //每一个子块的内存是否已分配，如果比特位是1，表示该子块已分配；
            //如果比特位是0，表示该子块未分配，标记顺序是其二进制数从低位到
            //高位进行排列。我们应该知道为什么bitmap大小要设置为子块数量除以
            //64，因为long类型的数字是64位，每一个元素都能记录64个子块的数
            //量，这样就可以通过SubPage个数除以64的方式决定bitmap中元素的数
            //量。如果子块不能整除64，则通过元素数量+1的方式，除以64之后剩余
            //的子块通过long中比特位由低到高进行排列记录，其逻辑结构如下图所
            //示。
            // index:0 ->[0|0|...|0|0|1]
            // index:1 ->[0|0|...|0|0|0]
            for (int i = 0; i < bitmapLength; i ++) {
                //bitmap标识哪个SubPage被分配
                //0表示未分配；1表示已分配
                bitmap[i] = 0;
            }
        }
        //加到Arena里：上面代码里的Head是Arena中数组tinySubpagePools中的元素，通
        //过以上逻辑，就会将新创建的SubPage以双向链表的形式关联到
        //tinySubpagePools中的元素，我们以16字节为例，关联关系如下图所示。
        //tinySubPagePools[32]
        // [0B]  [16B]  [32B]  [48B]... [496B]
        //        ↑↓
        //       [16B]
        // 下 次 如 果 还 需 要 分 配 16 字 节 的 内 存 ， 就 可 以 通 过
        //tinySubpagePools找到其元素关联的SubPage进行分配了
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     */
    long allocate() {
        if (elemSize == 0) {
            return toHandle(0);
        }

        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }
        //取一个bitmap中可用的id(绝对id)
        //其中bitmapIdx表示从bitmap中找到一个可用的比特位的下标。这里是比特位的下标，并不是数组的下标
        //我们之前分析过，由于每一比特位都代表一个子块的内存分配情况，通过这个下标就可以知道
        //哪个比特位是未分配状态。
        final int bitmapIdx = getNextAvail();
        //除以64(bitmap的相对下标)
        int q = bitmapIdx >>> 6;
        //除以64取余，其实就适当前绝对id的偏移量
        //表示获取bitmapIdx的位置是从当前元素最低位开始的第几个比特位
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) == 0;
        //当前位标记为1：将bitmap的位置设置为不可用，也就是比特位设置为1，表示已占用
        bitmap[q] |= 1L << r;
        //如果可用的SubP为0
        //可用的SubPage为-1
        //然后将可用于配置的数量numAvail减1。如果没有可用
        //SubPage的数量，则会将PoolArena中的数组tinySubpagePools所关联的
        //SubPage进行移除
        if (-- numAvail == 0) {
            //移除相关SubPage
            removeFromPool();
        }
        //bitmapIdx转换成handle：最后通过toHandle(bitmapIdx)获取当前子块的
        //Handle，上一小节我们知道Handle指向的是当前Chunk中唯一的一块内
        //存，我们跟进toHandle(bitmapIdx)
        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            return true;
        }
        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        bitmap[q] ^= 1L << r;

        setNextAvail(bitmapIdx);

        if (numAvail ++ == 0) {
            addToPool(head);
            return true;
        }

        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage not in use (numAvail == maxNumElems)
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }

            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        if (nextAvail >= 0) {
            //一个子SubPage被释放之后，会记录当前SubPage的bitmapIdx的位置，下次分配可以直接通过bitmapIdx获取一个SubPage
            this.nextAvail = -1;
            //上述代码片段中的nextAvail表示下一个可用的bitmapIdx，在释放
            //的时候会被标记，标记被释放的子块对应bitmapIdx的下标，如果<0代
            //表没有被释放的子块
            return nextAvail;
        }
        return findNextAvail();
    }

    private int findNextAvail() {
        //当前long数组
        final long[] bitmap = this.bitmap;
        //获取其长度
        final int bitmapLength = this.bitmapLength;
        for (int i = 0; i < bitmapLength; i ++) {
            //第i个
            long bits = bitmap[i];
            //!= -1 说明64位没有全部占满
            if (~bits != 0) {
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    /**
     * 这里会遍历bitmap中的每一个元素，如果当前元素中所有的比特位
     * 没有全部标记被使用，则通过findNextAvail0（i，bits）方法一个一
     * 个地往后找标记未使用的比特位。
     *
     * 下述代码从当前元素的第一个比特位开始找，直到找到一个标记为
     * 0的比特位，并返回当前比特位的下标
     *
     * @param i
     * @param bits
     * @return
     */
    private int findNextAvail0(int i, long bits) {
        //多少份
        final int maxNumElems = this.maxNumElems;
        //乘以64，代表当前long的第一个下标
        final int baseVal = i << 6;
        //循环64次(指当前的下标)
        for (int j = 0; j < 64; j ++) {
            //第一位为0(如果是2的倍数，则第一位就是0)
            if ((bits & 1) == 0) {
                //这里相当于加，将i*64之后加上j，获取绝对下标
                int val = baseVal | j;
                //小于块数(不能越界)
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            //当前下标不为0
            //右移一位
            bits >>>= 1;
        }
        return -1;
    }

    /**
     * （long）bitmapIdx<<32将bitmapIdx右移32位，而32位正好是一个
     * int的长度，这样通过（long）bitmapIdx<<32|memoryMapIdx计算，就
     * 可以将memoryMapIdx，也就是Page所属下标的二进制数保存在（long）
     * bitmapIdx<<32的低32位中。0x4000000000000000L是一个最高位是1并
     * 且所有低位都是0的二进制数，通过按位或的方式可以将（long）
     * bitmapIdx<<32|memoryMapIdx 计 算 出 来 的 结 果 保 存 在
     * 0x4000000000000000L的所有低位中，这样返回对的数字就可以指向
     * Chunk中唯一的一块内存
     * @param bitmapIdx
     * @return
     */
    private long toHandle(int bitmapIdx) {
        return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        synchronized (chunk.arena) {
            if (!this.doNotDestroy) {
                doNotDestroy = false;
                // Not used for creating the String.
                maxNumElems = numAvail = elemSize = -1;
            } else {
                doNotDestroy = true;
                maxNumElems = this.maxNumElems;
                numAvail = this.numAvail;
                elemSize = this.elemSize;
            }
        }

        if (!doNotDestroy) {
            return "(" + memoryMapIdx + ": not in use)";
        }

        return "(" + memoryMapIdx + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + pageSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        synchronized (chunk.arena) {
            return maxNumElems;
        }
    }

    @Override
    public int numAvailable() {
        synchronized (chunk.arena) {
            return numAvail;
        }
    }

    @Override
    public int elementSize() {
        synchronized (chunk.arena) {
            return elemSize;
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }
}
