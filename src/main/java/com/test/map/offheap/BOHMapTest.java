package com.test.map.offheap;

import sun.misc.Unsafe;

import java.util.Random;

public class BOHMapTest {

    public static void main(String[] args) {
        BinaryOffHeapMap map = new BinaryOffHeapMap(1024);

        byte[] array = randomArray(123);
        map.put(null, array);
        map.put(bytes(123), null);

        System.out.println(map.get(null).length);
        System.out.println(map.get(bytes(123)));
    }

    private static byte[] bytes(long value) {
        byte[] bytes = new byte[Long.BYTES];

        for (int i = 0; i < Long.BYTES; i++, value >>= Byte.SIZE) {
            bytes[i] = (byte) value;
        }

        return bytes;
    }

    private static byte[] randomArray(int size) {
        byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);

        return bytes;
    }

    public static void maian(String[] args) {
        int baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        int indexScale = Unsafe.ARRAY_BYTE_INDEX_SCALE;

        Unsafe unsafe = UnsafeUtil.getUnsafe();
        byte[] array = {(byte) 0xAB, (byte) 0xBC, (byte) 0xCD, (byte) 0xDE};

        int length = array.length;
        long address = unsafe.allocateMemory(length);

        unsafe.copyMemory(array, baseOffset, null, address, length);

        for (int i = 0; i < length; i++) {
            System.out.println(Integer.toHexString(Byte.toUnsignedInt(unsafe.getByte(address + i))));
        }
    }

    public static void memoryTest() {
        Unsafe unsafe = UnsafeUtil.getUnsafe();

        int size = 4;
        int byteSize = size * Long.BYTES;

        long address = unsafe.allocateMemory(byteSize);

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < Long.BYTES; j++) {
                long writeAddress = address + i * Long.BYTES + j;
                byte value = (byte) (j << i);
                unsafe.putByte(writeAddress, value);

                System.out.println(writeAddress + ": " + Integer.toHexString(Byte.toUnsignedInt(value)));
            }
            System.out.println();
        }

        for (int i = 0; i < size; i++) {
            long readAddress = address + i * Long.BYTES;
            System.out.println(address + ": " + Long.toHexString(unsafe.getLong(readAddress)));
        }

        System.out.println("---------------------------------------------------------");

        unsafe.freeMemory(address);
        address = unsafe.allocateMemory(byteSize);

        for (int i = 0; i < size; i++) {
            unsafe.putLong(address + i * Long.BYTES, 0xF0F1F2F300000000L + i);
        }

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < Long.BYTES; j++) {
                long readAddress = address + i * Long.BYTES + j;
                System.out.println(readAddress + ": " + Integer.toHexString(Byte.toUnsignedInt(unsafe.getByte(readAddress))));
            }
            System.out.println();
        }
    }
}
