package com.owl.kafka.proxy.server.biz.alloc;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * @Author: Tboy
 */
public class SimpleByteBufferPool implements ByteBufferPool {

    public static SimpleByteBufferPool I = new SimpleByteBufferPool();

    private static final TreeMap<Key, ByteBuffer> heapBuffers = new TreeMap<>();

    private static final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<>();

    private static final TreeMap<Key, ByteBuffer> getBuffer(boolean direct) {
        return direct ? directBuffers : heapBuffers;
    }

    private int chunkSize = 700;

    private int defaultPageSize = 2048;

    private int subpageOverflowMask = ~(defaultPageSize - 1);

    public synchronized ByteBuffer allocate(int capacity, boolean direct) {
        capacity = normalizeCapacity(capacity);
        //


        TreeMap<Key, ByteBuffer> treeMap = getBuffer(direct);
        Map.Entry<Key, ByteBuffer> entry = treeMap.ceilingEntry(new Key(capacity, 0));
        if (entry == null) {
            return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
        }
        treeMap.remove(entry.getKey());
        return entry.getValue();
    }

    public synchronized void release(ByteBuffer buffer) {
        TreeMap<Key, ByteBuffer> treeMap = getBuffer(buffer.isDirect());
        while (true) {
            Key key = new Key(buffer.capacity(), System.nanoTime());
            if (!treeMap.containsKey(key)) {
                treeMap.put(key, buffer);
                return;
            }
        }
    }

    private int normalizeCapacity(int capacity){
        if(capacity < 0){
            throw new IllegalArgumentException("capacity : " + capacity + " (expected : 0+)");
        }

        if(capacity >= chunkSize){
            return capacity;
        }

        if(!isTiny(capacity)){
            int normalizedCapacity = capacity;
            normalizedCapacity --;
            normalizedCapacity |= normalizedCapacity >>>  1;
            normalizedCapacity |= normalizedCapacity >>>  2;
            normalizedCapacity |= normalizedCapacity >>>  4;
            normalizedCapacity |= normalizedCapacity >>>  8;
            normalizedCapacity |= normalizedCapacity >>> 16;
            normalizedCapacity ++;

            if (normalizedCapacity < 0) {
                normalizedCapacity >>>= 1;
            }

            return normalizedCapacity;
        }

        if ((capacity & 15) == 0) {
            return capacity;
        }

        return (capacity & ~15) + 16;
    }

    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

    private int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i --) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    private int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < defaultPageSize) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + defaultPageSize + ")");
        }

        if ((pageSize & pageSize - 1) != 0) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
        }

        // Logarithm base 2. At this point we know that pageSize is a power of two.
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
    }

    private boolean isTiny(int capacity){
        return (capacity & 0xFFFFFE00) == 0;
    }

    //capacity < pageSize
    boolean isTinyOrSmall(int normCapacity) {
        return (normCapacity & subpageOverflowMask) == 0;
    }

    private static final class Key implements Comparable<Key> {
        private final int capacity;
        private final long insertionTime;

        Key(int capacity, long insertionTime) {
            this.capacity = capacity;
            this.insertionTime = insertionTime;
        }

        @Override
        public int compareTo(Key other) {
            return (this.capacity > other.capacity) ? 1 : (this.capacity < other.capacity) ? -1 :
                    (this.insertionTime > other.insertionTime ? 1 : (this.insertionTime < other.insertionTime ? -1 : 0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return capacity == key.capacity &&
                    insertionTime == key.insertionTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(capacity, insertionTime);
        }
    }

    public static void main(String[] args) {
        System.out.println(11 & ~(11 - 1));
        System.out.println(~10);
    }

}
