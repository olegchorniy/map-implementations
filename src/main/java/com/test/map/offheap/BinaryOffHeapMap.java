package com.test.map.offheap;

import sun.misc.Unsafe;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class BinaryOffHeapMap implements Map<byte[], byte[]> {

    //TODO: 2. buckets array chunking (cause it can growths to relative big size)
    //TODO: 3. keys/values chunking

    private static final Unsafe unsafe = UnsafeUtil.getUnsafe();

    private static final byte[] EMPTY_ARRAY = new byte[0];

    private static final int BYTE_ARRAY_OFFSET = Unsafe.ARRAY_BYTE_BASE_OFFSET;
    private static final int LONG_SIZE = Long.BYTES;
    private static final int INT_SIZE = Integer.BYTES;

    private static final int NULL_SIZE = -1;
    private static final int NO_ADDRESS = 0;

    private static final int DEFAULT_TABLE_SIZE = 16;

    private long size;
    private long tableAddress;
    private long mask;

    public BinaryOffHeapMap() {
        this(DEFAULT_TABLE_SIZE);
    }

    public BinaryOffHeapMap(long size) {
        long tableSize = Long.highestOneBit(size - 1) << 1;
        long sizeInBytes = LONG_SIZE * tableSize;

        this.tableAddress = unsafe.allocateMemory(sizeInBytes);
        unsafe.setMemory(this.tableAddress, sizeInBytes, (byte) 0);

        this.mask = tableSize - 1; //tableSize = 00...0100...0 => mask = 00...0011...1
        this.size = 0;
    }

    @Override
    public int size() {
        return (int) size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public byte[] get(Object keyObj) {
        if (keyObj != null && !(keyObj instanceof byte[])) {
            return null;
        }

        byte[] key = (byte[]) keyObj;

        final long index = indexFor(hash(key));
        final long bucketAddress = bucketAddress(index);

        final long headNodeAddress = unsafe.getLong(bucketAddress);

        if (headNodeAddress == 0) {
            return null;
        }

        for (long nodeAddress = headNodeAddress; nodeAddress != 0; ) {
            Node node = readNode(nodeAddress);

            if (equalKeys(node, key)) {
                return readArray(node.valueAddress, node.valueSize, false);
            }

            nodeAddress = node.nextNodeAddress;
        }

        //not found
        return null;
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        final long hash = hash(key);
        final long index = indexFor(hash);

        final long bucketAddress = bucketAddress(index);
        final long headNodeAddress = unsafe.getLong(bucketAddress);

        if (headNodeAddress == 0) {
            //empty bucket
            long newNodeAddress = createNewNode(key, value, hash);
            unsafe.putLong(bucketAddress, newNodeAddress);

            size++;
            return null;
        }

        long prevNodeAddress;
        long currentNodeAddress = headNodeAddress;
        do {
            Node currentNode = readNode(currentNodeAddress);

            if (equalKeys(currentNode, key)) {
                //found node with the same key - replace old value with a new one
                return replaceValue(currentNodeAddress, currentNode.valueAddress, currentNode.valueSize, value);
            }

            prevNodeAddress = currentNodeAddress;
            currentNodeAddress = currentNode.nextNodeAddress;
        } while (currentNodeAddress != 0);

        //append new node to the end of the chain
        long newNodeAddress = createNewNode(key, value, hash);
        setNextNodeAddress(prevNodeAddress, newNodeAddress);

        size++;
        return null;
    }

    @Override
    public byte[] remove(Object keyObj) {
        if (keyObj != null && !(keyObj instanceof byte[])) {
            return null;
        }

        byte[] key = (byte[]) keyObj;

        final long index = indexFor(hash(key));
        final long bucketAddress = bucketAddress(index);

        final long headNodeAddress = unsafe.getLong(bucketAddress);

        if (headNodeAddress == 0) {
            return null;
        }

        long prevNodeAddress = 0; //needed for chain adjustment

        for (long nodeAddress = headNodeAddress; nodeAddress != 0; ) {
            Node node = readNode(nodeAddress);

            if (equalKeys(node, key)) {
                byte[] value = readArray(node.valueAddress, node.valueSize, true /* deallocate memory occupied by value */);

                if (prevNodeAddress == 0) {
                    //key found in the head node
                    unsafe.putLong(bucketAddress, node.nextNodeAddress);
                } else {
                    setNextNodeAddress(prevNodeAddress, node.nextNodeAddress);
                }

                //value memory is already freed, do the same thing with the memory allocated for a key and for a node itself
                unsafe.freeMemory(node.keyAddress);
                unsafe.freeMemory(nodeAddress);

                size--;
                return value;
            }

            prevNodeAddress = nodeAddress;
            nodeAddress = node.nextNodeAddress;
        }

        //not found
        return null;
    }

    private static boolean equalKeys(Node node, byte[] key) {
        //1. check for null equality
        boolean nodeKeyIsNull = node.keySize == NULL_SIZE;
        boolean keyIsNull = key == null;

        if (nodeKeyIsNull && keyIsNull) {
            //both keys are null
            return true;
        } else if (nodeKeyIsNull ^ keyIsNull) {
            //only one of the keys is null
            return false;
        }

        //2. compare size
        if (node.keySize != key.length) {
            return false;
        }

        //3. compare content
        for (int i = 0; i < key.length; i++) {
            if (key[i] != unsafe.getByte(node.keyAddress + i)) {
                return false;
            }
        }

        //size and content are equal => keys are equal
        return true;
    }

    @Override
    public void putAll(Map<? extends byte[], ? extends byte[]> m) {

    }

    @Override
    public void clear() {

    }

    /* ----------------- Utility methods for hash/index manipulations ----------------- */

    private long bucketAddress(long index) {
        return tableAddress + index * LONG_SIZE;
    }

    private static long hash(byte[] key) {
        return Arrays.hashCode(key);
    }

    private long indexFor(long hash) {
        return hash & mask;
    }

    @Override
    public Set<byte[]> keySet() {
        return null;
    }

    @Override
    public Collection<byte[]> values() {
        return null;
    }

    @Override
    public Set<Entry<byte[], byte[]>> entrySet() {
        return null;
    }

    /*
     * Node memory layout:
     * <p>
     * |<------ 8 bytes ------>|
     * +-----------------------+   ^
     * |      Key address      |   |
     * +-----------------------+   |
     * |     Value address     |   |
     * +----------+------------+   |
     * | Key size | Value size |   | decreasing
     * +----------+------------+   | addresses
     * |   Next node address   |   |
     * +-----------------------+   |
     * |          Hash         |   |
     * +-----------------------+   |
     */

    private static long storeNode(Node node) {
        final long address = unsafe.allocateMemory(Node.NODE_SIZE);
        long writeAddress = address;

        unsafe.putLong(writeAddress, node.keyAddress);
        unsafe.putLong(writeAddress += LONG_SIZE, node.valueAddress);
        unsafe.putInt(writeAddress += LONG_SIZE, node.keySize);
        unsafe.putInt(writeAddress += INT_SIZE, node.valueSize);
        unsafe.putLong(writeAddress += INT_SIZE, node.nextNodeAddress);
        unsafe.putLong(writeAddress += LONG_SIZE, node.hash);

        return address;
    }

    private static Node readNode(long address) {
        return new Node(
                unsafe.getLong(address),
                unsafe.getLong(address += LONG_SIZE),
                unsafe.getInt(address += LONG_SIZE),
                unsafe.getInt(address += INT_SIZE),
                unsafe.getLong(address += INT_SIZE),
                unsafe.getLong(address += LONG_SIZE)
        );
    }

    private static long createNewNode(byte[] key, byte[] value, long computedHash) {
        SizeAndAddress keyInfo = storeArray(key);
        SizeAndAddress valueInfo = storeArray(value);

        return storeNode(new Node(keyInfo.address, valueInfo.address, keyInfo.size, valueInfo.size, 0, computedHash));
    }

    private static byte[] replaceValue(long nodeAddress, long oldValueAddress, int oldValueSize, byte[] newValue) {
        SizeAndAddress info = storeArray(newValue);

        //update value address and value size information in already stored node
        unsafe.putLong(nodeAddress + Node.VALUE_ADDRESS_OFFSET, info.address);
        unsafe.putInt(nodeAddress + Node.VALUE_SIZE_OFFSET, info.size);

        //read old value and deallocate memory
        return readArray(oldValueAddress, oldValueSize, true /* deallocate */);
    }

    private static void setNextNodeAddress(long prevNodeAddress, long nextNodeAddress) {
        unsafe.putLong(prevNodeAddress + Node.NEXT_NODE_ADDRESS_OFFSET, nextNodeAddress);
    }

    private static class Node {

        static final int NODE_SIZE = 4 * LONG_SIZE + 2 * INT_SIZE;

        static final int VALUE_ADDRESS_OFFSET = LONG_SIZE;
        static final int VALUE_SIZE_OFFSET = VALUE_ADDRESS_OFFSET + LONG_SIZE + INT_SIZE;
        static final int NEXT_NODE_ADDRESS_OFFSET = VALUE_SIZE_OFFSET + INT_SIZE;

        final long keyAddress;
        final long valueAddress;
        final int keySize;
        final int valueSize;
        final long nextNodeAddress;
        final long hash;

        Node(long keyAddress, long valueAddress, int keySize, int valueSize, long nextNodeAddress, long hash) {
            this.keyAddress = keyAddress;
            this.valueAddress = valueAddress;
            this.keySize = keySize;
            this.valueSize = valueSize;
            this.nextNodeAddress = nextNodeAddress;
            this.hash = hash;
        }
    }

    /* ----------------- Byte array storing/reading routines ----------------- */

    private static SizeAndAddress storeArray(byte[] value) {
        int size;
        long address;

        if (value == null) {
            size = NULL_SIZE;
            address = NO_ADDRESS;
        } else {
            size = value.length;
            if (size == 0) {
                address = NO_ADDRESS;
            } else {
                address = unsafe.allocateMemory(size);
                unsafe.copyMemory(value, BYTE_ARRAY_OFFSET, null, address, value.length);
            }
        }

        return new SizeAndAddress(size, address);
    }

    private static byte[] readArray(long address, int size, boolean deallocate) {
        if (size == NULL_SIZE) {
            return null;
        }

        if (size == 0) {
            return EMPTY_ARRAY;
        }

        byte[] array = new byte[size];
        unsafe.copyMemory(null, address, array, BYTE_ARRAY_OFFSET, size);

        if (deallocate) {
            unsafe.freeMemory(address);
        }

        return array;
    }

    private static class SizeAndAddress {
        final int size;
        final long address;

        SizeAndAddress(int size, long address) {
            this.size = size;
            this.address = address;
        }
    }
}
