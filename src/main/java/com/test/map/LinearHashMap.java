package com.test.map;

import java.util.Objects;

@SuppressWarnings("unchecked")
public class LinearHashMap<K, V> implements SimpleMap<K, V> {

    private static final int BUCKET_HASH_BITS = 4;
    private static final int SEGMENT_SIZE = 1 << BUCKET_HASH_BITS;
    private static final int MAX_SEGMENTS = 1 << 16;

    private final Node<K, V>[][] segments;
    private int size;

    private final double maxLoadFactor;

    // Invariant: 0 <= splitIndex < 2 ^ (hashBits - 1) = 1 << (hashBits - 1)
    private int hashBits;
    private int splitIndex;

    public LinearHashMap() {
        this(1 << 6, 0.75);
    }

    public LinearHashMap(int initialSize, double maxLoadFactor) {
        int bucketsNum = (initialSize == 1)
                ? 1
                : (Integer.highestOneBit(initialSize - 1) << 1);

        int segmentsNum = bucketsNum >>> BUCKET_HASH_BITS;

        // TODO: min or max ? Hmmm
        this.segments = new Node[Math.max(segmentsNum, MAX_SEGMENTS)][];
        this.maxLoadFactor = maxLoadFactor;
        this.hashBits = Integer.SIZE - Integer.numberOfLeadingZeros(bucketsNum);
        this.splitIndex = 0;
    }

    @Override
    public V get(K key) {
        int hash = hash(key);

        int index = indexFor(hash);
        int segmentIndex = segmentIndex(index);
        int bucketIndex = bucketIndex(index);

        Node<K, V>[] segment = this.segments[segmentIndex];
        if (segment == null) {
            return null;
        }

        Node<K, V> node = segment[bucketIndex];
        if (node == null) {
            return null;
        }

        do {
            if (node.keyEqualsTo(key, hash)) {
                return node.value;
            }

            node = node.next;
        } while (node != null);

        return null;
    }

    @Override
    public V put(K key, V value) {
        int hash = hash(key);

        int index = indexFor(hash);
        int segmentIndex = segmentIndex(index);
        int bucketIndex = bucketIndex(index);

        Node<K, V>[] segment = getOrCreateSegment(segmentIndex);
        Node<K, V> node = segment[bucketIndex];

        if (node == null) {
            segment[bucketIndex] = new Node<>(key, hash, value);

            this.size++;
            split();

            return null;
        }

        Node<K, V> prev;
        do {
            if (node.keyEqualsTo(key, hash)) {
                V oldValue = node.value;
                node.value = value;

                return oldValue;
            }

            prev = node;
            node = node.next;
        } while (node != null);

        prev.next = new Node<>(key, hash, value);

        this.size++;
        split();

        return null;
    }

    @Override
    public V remove(K key) {
        int hash = hash(key);

        int index = indexFor(hash);
        int segmentIndex = segmentIndex(index);
        int bucketIndex = bucketIndex(index);

        Node<K, V>[] segment = this.segments[segmentIndex];
        if (segment == null) {
            return null;
        }

        Node<K, V> node = segment[bucketIndex];
        if (node == null) {
            return null;
        }

        Node<K, V> prev = null;
        do {
            if (!node.keyEqualsTo(key, hash)) {
                prev = node;
                node = node.next;

                continue;
            }

            if (prev == null) {
                // head of the list
                segment[bucketIndex] = node.next;
            } else {
                prev.next = node.next;
            }

            this.size--;

            return node.value;
        } while (node != null);

        return null;
    }

    private void split() {
        if (this.loadFactor() < this.maxLoadFactor) {
            return;
        }

        final int splitSegmentIndex = segmentIndex(this.splitIndex);
        // TODO: bucketIndex(...) call seems to be redundant here and below
        final int splitBucketIndex = bucketIndex(this.splitIndex);

        final Node<K, V>[] segment = this.segments[splitSegmentIndex];
        if (segment == null) {
            incSplitIndex();
            return;
        }

        final Node<K, V> head = segment[splitBucketIndex];
        if (head == null) {
            incSplitIndex();
            return;
        }

        final int edgeBit = 1 << (this.hashBits - 1);
        final int buddyIndex = this.splitIndex + edgeBit; // splitIndex + 2 ^ (hashBits - 1)
        final int buddySegmentIndex = segmentIndex(buddyIndex);
        final int buddyBucketIndex = bucketIndex(buddyIndex);

        Node<K, V> prev = null;
        Node<K, V> node = head;
        Node<K, V> buddyHead = null;

        do {
            final Node<K, V> next = node.next;

            if ((node.hash & edgeBit) != 0) {

                // 1. Exclude node from the 0th list
                if (prev == null) {
                    segment[splitBucketIndex] = next;
                } else {
                    prev.next = next;
                }

                // 2. Append node to the 1th list
                node.next = buddyHead;
                buddyHead = node;
            } else {
                prev = node;
            }

            node = next;
        } while (node != null);

        if (buddyHead != null) {
            Node<K, V>[] buddySegment = getOrCreateSegment(buddySegmentIndex);
            buddySegment[buddyBucketIndex] = buddyHead;
        }

        incSplitIndex();
    }

    private void incSplitIndex() {
        this.splitIndex++;

        if (this.splitIndex == (1 << (this.hashBits - 1))) {
            this.hashBits++;
            this.splitIndex = 0;
        }
    }

    private Node<K, V>[] getOrCreateSegment(int index) {
        Node<K, V>[] segment = this.segments[index];

        if (segment == null) {
            this.segments[index] = segment = newSegment();
        }

        return segment;
    }

    private int hash(Object key) {
        if (key == null) {
            return 0;
        }
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    private int indexFor(int hash) {
        int fullIndex = hash & mask(this.hashBits);
        int halfIndex = fullIndex & ~(1 << (this.hashBits - 1));

        return halfIndex < this.splitIndex ? fullIndex : halfIndex;
    }

    private int bucketIndex(int index) {
        return index & mask(BUCKET_HASH_BITS);
    }

    private int segmentIndex(int index) {
        return index >>> BUCKET_HASH_BITS;
    }

    private static int mask(int nBits) {
        return (1 << nBits) - 1;
    }

    private Node<K, V>[] newSegment() {
        return new Node[SEGMENT_SIZE];
    }

    private double loadFactor() {
        return this.size / ((double) bucketsNum());
    }

    private int bucketsNum() {
        return (1 << (this.hashBits - 1)) + this.splitIndex;
    }

    public void printDebug() {
        System.out.printf("Hash bits: %d, split index: %d%n", this.hashBits, this.splitIndex);

        int maxChainLength = 0;

        for (int i = 0; i < segments.length; i++) {
            Node<K, V>[] segment = segments[i];
            if (segment == null) {
                continue;
            }

            System.out.format("---------- Segment #%d ----------%n", i);

            for (int j = 0; j < segment.length; j++) {
                System.out.printf("\tBucket #%x: ", (i * segment.length) + j);

                Node<K, V> head = segment[j];
                if (head == null) {
                    System.out.printf("[empty]%n");
                    continue;
                }

                int chainLength = 0;

                for (Node<K, V> node = head; node != null; node = node.next) {
                    System.out.printf("[%s: %s]", node.key, node.value);
                    if (node.next != null) {
                        System.out.print(" -> ");
                    }

                    chainLength++;
                }

                System.out.println();

                if (chainLength > maxChainLength) {
                    maxChainLength = chainLength;
                }
            }
        }

        System.out.printf(
                "Max chain length = %d, load factor = %.3f%n",
                maxChainLength, loadFactor()
        );
    }

    private static class Node<K, V> {

        final K key;
        final int hash;
        V value;

        Node<K, V> next;

        Node(K key, int hash, V value) {
            this.key = key;
            this.hash = hash;
            this.value = value;
        }

        boolean keyEqualsTo(K key, int keyHash) {
            return this.hash == keyHash && Objects.equals(this.key, key);
        }
    }
}
