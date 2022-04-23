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


import io.netty.buffer.PoolArena.SizeClass;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.util.Queue;

/**
 * Acts a Thread cache for allocations. This implementation is moduled after
 * <a href="http://people.freebsd.org/~jasone/jemalloc/bsdcan2006/jemalloc.pdf">jemalloc</a> and the descripted
 * technics of
 * <a href="https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919">
 * Scalable memory allocation using jemalloc</a>.
 *
 * 那么PoolThreadCache除了可以在Arena上进行内存分配，还可以在它底层维护的ByteBuf缓存列表进行分配。
 * 举个例子：我们通过
 * PooledByteBufAllocator创建了一个1024字节的ByteBuf，当用完释放
 * 后，可能在其他地方会继续分配1024字节的ByteBuf。这时，其实不需
 * 要在Arena上进行内存分配，而是直接通过PoolThreadCache中维护的
 * ByteBuf的缓存列表直接拿过来返回。在PooledByteBufAllocator中维
 * 护 着 三 种 规 格 大 小 的 缓 存 列 表 ， 分 别 是 三 个 值 tinyCacheSize 、
 * smallCacheSize、normalCacheSize
 */
final class PoolThreadCache {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PoolThreadCache.class);

    final PoolArena<byte[]> heapArena;
    final PoolArena<ByteBuffer> directArena;

    // Hold the caches for the different size classes, which are tiny, small and normal.
    private final MemoryRegionCache<byte[]>[] tinySubPageHeapCaches;
    private final MemoryRegionCache<byte[]>[] smallSubPageHeapCaches;
    private final MemoryRegionCache<byte[]>[] normalHeapCaches;
    //不同类型的缓存数组规格不一样，
    // tiny类型的数组长度是32，
    // small类型的数组长度是4，
    // normal类型的数组长度是3。
    //缓存数组中的每一个元素都是MemoryRegionCache类型，代表一个缓存对象。
    //每个MemoryRegionCache对象中维护了一个队列，队列的容量大小由
    //PooledByteBufAllocator 类 中 定 义 的 tinyCacheSize 、
    //smallCacheSize、normalCacheSize的值来决定。

    //MemoryRegionCache对象的队列中的元素为ByteBuf类型，ByteBuf
    //的大小也是固定的。这样，Netty就将每种ByteBuf的容量大小划分成了
    //不同的规格。同一个队列中，每个ByteBuf的容量大小是相同的规格。
    //比如，在tiny类型中，Netty将其长度分成32种规格，每种规格都是16
    //的整数倍，也就是包含0Byte、16Byte、32Byte、48Byte、64Byte、
    //80Byte、96Byte……496Byte，总共32种规格。
    private final MemoryRegionCache<ByteBuffer>[] tinySubPageDirectCaches;//tiny类型
    //● tiny：共32种规格，均是16的整数倍，0Byte、16Byte、32Byte、48Byte、64Byte、80Byte、96Byte……496Byte。
    //● small：4种规格，512Byte、1KB、2KB、4KB。
    private final MemoryRegionCache<ByteBuffer>[] smallSubPageDirectCaches;//small类型
    //● normal：3种规格，8KB、16KB、32KB。
    private final MemoryRegionCache<ByteBuffer>[] normalDirectCaches;//normal类型

    // Used for bitshifting when calculate the index of normal caches later
    private final int numShiftsNormalDirect;
    private final int numShiftsNormalHeap;
    private final int freeSweepAllocationThreshold;

    private int allocations;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
                    int tinyCacheSize, int smallCacheSize, int normalCacheSize,
                    int maxCachedBufferCapacity, int freeSweepAllocationThreshold) {
        if (maxCachedBufferCapacity < 0) {
            throw new IllegalArgumentException("maxCachedBufferCapacity: "
                    + maxCachedBufferCapacity + " (expected: >= 0)");
        }
        this.freeSweepAllocationThreshold = freeSweepAllocationThreshold;
        this.heapArena = heapArena;
        this.directArena = directArena;
        if (directArena != null) {
            //在构造方法中对这三种类型进行初始化
            //在PoolThreadCache给数组tinySubPageDirectCaches赋值前，需要
            //设 定 的 数 组 长 度 就 是 对 应 每 一 种 规 格 的 固 定 值 。 以
            //tinySubPageDirectCaches[1]为例（下标选择1是因为下标为0代表的规
            //格是0Byte，其实就代表一个空的缓存，这里不进行举例）。
            //在tinySubPageDirectCaches[1]的缓存对象中所缓存的ByteBuf的缓冲区
            //大小是16Byte，在tinySubPageDirectCaches[2]中缓存的ByteBuf大小
            //为32Byte，依此类推，tinySubPageDirectCaches[31]中缓存的ByteBuf
            //大小为496Byte。
            //不同类型的缓存数组规格不一样:
            // tiny类型的数组长度是32，
            // small类型的数组长度是4，
            // normal类型的数组长度是3。
            tinySubPageDirectCaches = createSubPageCaches(
                    tinyCacheSize, PoolArena.numTinySubpagePools, SizeClass.Tiny);
            smallSubPageDirectCaches = createSubPageCaches(
                    smallCacheSize, directArena.numSmallSubpagePools, SizeClass.Small);

            numShiftsNormalDirect = log2(directArena.pageSize);
            normalDirectCaches = createNormalCaches(
                    normalCacheSize, maxCachedBufferCapacity, directArena);

            directArena.numThreadCaches.getAndIncrement();
        } else {
            // No directArea is configured so just null out all caches
            tinySubPageDirectCaches = null;
            smallSubPageDirectCaches = null;
            normalDirectCaches = null;
            numShiftsNormalDirect = -1;
        }
        if (heapArena != null) {
            // Create the caches for the heap allocations
            tinySubPageHeapCaches = createSubPageCaches(
                    tinyCacheSize, PoolArena.numTinySubpagePools, SizeClass.Tiny);
            smallSubPageHeapCaches = createSubPageCaches(
                    smallCacheSize, heapArena.numSmallSubpagePools, SizeClass.Small);

            numShiftsNormalHeap = log2(heapArena.pageSize);
            normalHeapCaches = createNormalCaches(
                    normalCacheSize, maxCachedBufferCapacity, heapArena);

            heapArena.numThreadCaches.getAndIncrement();
        } else {
            // No heapArea is configured so just null out all caches
            tinySubPageHeapCaches = null;
            smallSubPageHeapCaches = null;
            normalHeapCaches = null;
            numShiftsNormalHeap = -1;
        }

        // Only check if there are caches in use.
        if ((tinySubPageDirectCaches != null || smallSubPageDirectCaches != null || normalDirectCaches != null
                || tinySubPageHeapCaches != null || smallSubPageHeapCaches != null || normalHeapCaches != null)
                && freeSweepAllocationThreshold < 1) {
            throw new IllegalArgumentException("freeSweepAllocationThreshold: "
                    + freeSweepAllocationThreshold + " (expected: > 0)");
        }
    }

    private static <T> MemoryRegionCache<T>[] createSubPageCaches(
            int cacheSize, int numCaches, SizeClass sizeClass) {
        if (cacheSize > 0 && numCaches > 0) {
            @SuppressWarnings("unchecked")
            //创建了一个缓存数组，这个缓存数组的长度是numCaches
            MemoryRegionCache<T>[] cache = new MemoryRegionCache[numCaches];
            for (int i = 0; i < cache.length; i++) {
                // TODO: maybe use cacheSize / cache.length
                // 需要设 定 的 数 组 长 度 就 是 对 应 每 一 种 规 格 的 固 定 值
                //tinySubPageDirectCaches[1]为例（下标选择1是因为下标为0代表的规
                //格是0Byte，其实就代表一个空的缓存，这里不进行举例）
                cache[i] = new SubPageMemoryRegionCache<T>(cacheSize, sizeClass);
            }
            return cache;
        } else {
            return null;
        }
    }

    private static <T> MemoryRegionCache<T>[] createNormalCaches(
            int cacheSize, int maxCachedBufferCapacity, PoolArena<T> area) {
        if (cacheSize > 0 && maxCachedBufferCapacity > 0) {
            int max = Math.min(area.chunkSize, maxCachedBufferCapacity);
            int arraySize = Math.max(1, log2(max / area.pageSize) + 1);

            @SuppressWarnings("unchecked")
            MemoryRegionCache<T>[] cache = new MemoryRegionCache[arraySize];
            for (int i = 0; i < cache.length; i++) {
                cache[i] = new NormalMemoryRegionCache<T>(cacheSize);
            }
            return cache;
        } else {
            return null;
        }
    }

    private static int log2(int val) {
        int res = 0;
        while (val > 1) {
            val >>= 1;
            res++;
        }
        return res;
    }

    /**
     * Try to allocate a tiny buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateTiny(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        //cacheForTiny 其作用是根据 normCapacity 找 到 tiny 类 型 缓 存 数 组 中 的 一 个 缓 存 对 象 。
        return allocate(cacheForTiny(area, normCapacity), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateSmall(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForSmall(area, normCapacity), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateNormal(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForNormal(area, normCapacity), buf, reqCapacity);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private boolean allocate(MemoryRegionCache<?> cache, PooledByteBuf buf, int reqCapacity) {
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        boolean allocated = cache.allocate(buf, reqCapacity);
        if (++ allocations >= freeSweepAllocationThreshold) {
            allocations = 0;
            trim();
        }
        return allocated;
    }

    /**
     * Add {@link PoolChunk} and {@code handle} to the cache if there is enough room.
     * Returns {@code true} if it fit into the cache {@code false} otherwise.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean add(PoolArena<?> area, PoolChunk chunk, long handle, int normCapacity, SizeClass sizeClass) {
        //获取MemoryRegionCache节点
        MemoryRegionCache<?> cache = cache(area, normCapacity, sizeClass);
        if (cache == null) {
            return false;
        }
        //将Chunk和Handle封装成实体加到Queue里面
        return cache.add(chunk, handle);
    }

    /**
     * 这里会根据不同的内存规格去找不同的对象，我们简单回顾一下，每个缓存对象都包含一个
     * Queue，Queue中每个节点都是Entry，每一个Entry中都包含一个Chunk
     * 和Handle，可以指向唯一连续的内存。
     *
     * @param area
     * @param normCapacity
     * @param sizeClass
     * @return
     */
    private MemoryRegionCache<?> cache(PoolArena<?> area, int normCapacity, SizeClass sizeClass) {
        switch (sizeClass) {
        case Normal:
            return cacheForNormal(area, normCapacity);
        case Small:
            return cacheForSmall(area, normCapacity);
        case Tiny:
            return cacheForTiny(area, normCapacity);
        default:
            throw new Error();
        }
    }

    /**
     *  Should be called if the Thread that uses this cache is about to exist to release resources out of the cache
     */
    void free() {
        int numFreed = free(tinySubPageDirectCaches) +
                free(smallSubPageDirectCaches) +
                free(normalDirectCaches) +
                free(tinySubPageHeapCaches) +
                free(smallSubPageHeapCaches) +
                free(normalHeapCaches);

        if (numFreed > 0 && logger.isDebugEnabled()) {
            logger.debug("Freed {} thread-local buffer(s) from thread: {}", numFreed, Thread.currentThread().getName());
        }

        if (directArena != null) {
            directArena.numThreadCaches.getAndDecrement();
        }

        if (heapArena != null) {
            heapArena.numThreadCaches.getAndDecrement();
        }
    }

    private static int free(MemoryRegionCache<?>[] caches) {
        if (caches == null) {
            return 0;
        }

        int numFreed = 0;
        for (MemoryRegionCache<?> c: caches) {
            numFreed += free(c);
        }
        return numFreed;
    }

    private static int free(MemoryRegionCache<?> cache) {
        if (cache == null) {
            return 0;
        }
        return cache.free();
    }

    void trim() {
        trim(tinySubPageDirectCaches);
        trim(smallSubPageDirectCaches);
        trim(normalDirectCaches);
        trim(tinySubPageHeapCaches);
        trim(smallSubPageHeapCaches);
        trim(normalHeapCaches);
    }

    private static void trim(MemoryRegionCache<?>[] caches) {
        if (caches == null) {
            return;
        }
        for (MemoryRegionCache<?> c: caches) {
            trim(c);
        }
    }

    private static void trim(MemoryRegionCache<?> cache) {
        if (cache == null) {
            return;
        }
        cache.trim();
    }

    /**
     * 这个方法我们之前剖析过，就是根据大小找到对应的缓存，获得下
     * 标之后，通过Cache找相对应的缓存对象。
     *
     * @param area
     * @param normCapacity
     * @return
     */
    private MemoryRegionCache<?> cacheForTiny(PoolArena<?> area, int normCapacity) {
        int idx = PoolArena.tinyIdx(normCapacity);
        if (area.isDirect()) {
            return cache(tinySubPageDirectCaches, idx);
        }
        return cache(tinySubPageHeapCaches, idx);
    }

    private MemoryRegionCache<?> cacheForSmall(PoolArena<?> area, int normCapacity) {
        int idx = PoolArena.smallIdx(normCapacity);
        if (area.isDirect()) {
            return cache(smallSubPageDirectCaches, idx);
        }
        return cache(smallSubPageHeapCaches, idx);
    }

    private MemoryRegionCache<?> cacheForNormal(PoolArena<?> area, int normCapacity) {
        if (area.isDirect()) {
            int idx = log2(normCapacity >> numShiftsNormalDirect);
            return cache(normalDirectCaches, idx);
        }
        int idx = log2(normCapacity >> numShiftsNormalHeap);
        return cache(normalHeapCaches, idx);
    }

    private static <T> MemoryRegionCache<T> cache(MemoryRegionCache<T>[] cache, int idx) {
        if (cache == null || idx > cache.length - 1) {
            return null;
        }
        //直接通过下标获取的缓存对象
        return cache[idx];
    }

    /**
     * Cache used for buffers which are backed by TINY or SMALL size.
     */
    private static final class SubPageMemoryRegionCache<T> extends MemoryRegionCache<T> {
        SubPageMemoryRegionCache(int size, SizeClass sizeClass) {
            super(size, sizeClass);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            chunk.initBufWithSubpage(buf, handle, reqCapacity);
        }
    }

    /**
     * Cache used for buffers which are backed by NORMAL size.
     */
    private static final class NormalMemoryRegionCache<T> extends MemoryRegionCache<T> {
        NormalMemoryRegionCache(int size) {
            super(size, SizeClass.Normal);
        }

        @Override
        protected void initBuf(
                PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            chunk.initBuf(buf, handle, reqCapacity);
        }
    }

    private abstract static class MemoryRegionCache<T> {
        private final int size;
        private final Queue<Entry<T>> queue;
        private final SizeClass sizeClass;
        private int allocations;

        MemoryRegionCache(int size, SizeClass sizeClass) {
            this.size = MathUtil.safeFindNextPositivePowerOfTwo(size);
            //底层数据结构是一个循环固定长度队列，且用了字节填充的方式，防止共享缓存行。
            //在异步操作中可以原子更新数据。
            queue = PlatformDependent.newFixedMpscQueue(this.size);
            this.sizeClass = sizeClass;
        }

        /**
         * Init the {@link PooledByteBuf} using the provided chunk and handle with the capacity restrictions.
         */
        protected abstract void initBuf(PoolChunk<T> chunk, long handle,
                                        PooledByteBuf<T> buf, int reqCapacity);

        /**
         * Add to cache if not already full.
         *
         * 在缓存中分配的时候从Queue弹出一个Entry，会
         * 放到一个对象池里面，而Entry<T> entry=newEntry(chunk，handle)就
         * 是从对象池里取一个Entry对象，然后将Chunk和Handle赋值，通过
         * queue.offer(entry)加到Queue
         *
         */
        @SuppressWarnings("unchecked")
        public final boolean add(PoolChunk<T> chunk, long handle) {
            Entry<T> entry = newEntry(chunk, handle);
            boolean queued = queue.offer(entry);
            if (!queued) {
                // If it was not possible to cache the chunk, immediately recycle the entry
                entry.recycle();
            }

            return queued;
        }

        /**
         * Allocate something out of the cache if possible and remove the entry from the cache.
         */
        public final boolean allocate(PooledByteBuf<T> buf, int reqCapacity) {
            //首先通过queue.poll()方法弹出一个Entry，我们
            //知道MemoryRegionCache内部维护着一个队列，而队列中的每一个值是一个Entry。
            Entry<T> entry = queue.poll();
            if (entry == null) {
                return false;
            }
            //方法初始化ByteBuf，这里参数传入Entry的Chunk和Handle
            initBuf(entry.chunk, entry.handle, buf, reqCapacity);
            //entry.recycle()这步将Entry对象回收，因为Entry对象弹出之后没有再被引用，所以可能GC会
            //将Entry对象回收。Netty为了将对象循环利用，将其放在对象回收站进行回收。
            entry.recycle();

            // allocations is not thread-safe which is fine as this is only called from the same thread all time.
            ++ allocations;
            return true;
        }

        /**
         * Clear out this cache and free up all previous cached {@link PoolChunk}s and {@code handle}s.
         */
        public final int free() {
            return free(Integer.MAX_VALUE);
        }

        private int free(int max) {
            int numFreed = 0;
            for (; numFreed < max; numFreed++) {
                Entry<T> entry = queue.poll();
                if (entry != null) {
                    freeEntry(entry);
                } else {
                    // all cleared
                    return numFreed;
                }
            }
            return numFreed;
        }

        /**
         * Free up cached {@link PoolChunk}s if not allocated frequently enough.
         */
        public final void trim() {
            int free = size - allocations;
            allocations = 0;

            // We not even allocated all the number that are
            if (free > 0) {
                free(free);
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private  void freeEntry(Entry entry) {
            PoolChunk chunk = entry.chunk;
            long handle = entry.handle;

            // recycle now so PoolChunk can be GC'ed.
            entry.recycle();

            chunk.arena.freeChunk(chunk, handle, sizeClass);
        }

        static final class Entry<T> {
            //通过Chunk和Handle就可以定位ByteBuf中指定的一块连续内存
            final Handle recyclerHandle;//Handle相当于一个指针，可以唯一定位Chunk里的一块连续内存
            PoolChunk<T> chunk;//Chunk代表一块连续的内存，Netty是以Chunk(16MB)为单位进行内存分配的
            long handle = -1;

            Entry(Handle recyclerHandle) {
                this.recyclerHandle = recyclerHandle;
            }

            void recycle() {
                //表示当前Entry不指向任何一块内存
                chunk = null;
                handle = -1;
                //将当前Entry回收,最终是放到一个Stack栈结构中
                RECYCLER.recycle(this, recyclerHandle);
            }
        }

        @SuppressWarnings("rawtypes")
        private static Entry newEntry(PoolChunk<?> chunk, long handle) {
            Entry entry = RECYCLER.get();
            entry.chunk = chunk;
            entry.handle = handle;
            return entry;
        }

        @SuppressWarnings("rawtypes")
        private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
            @Override
            protected Entry newObject(Handle handle) {
                return new Entry(handle);
            }
        };
    }
}
