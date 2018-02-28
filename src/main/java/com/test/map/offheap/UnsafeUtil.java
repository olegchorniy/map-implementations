package com.test.map.offheap;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {
    private UnsafeUtil() {
    }

    public static Unsafe getUnsafe() {
        try {
            Field theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            return (Unsafe) theUnsafeField.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new Error(e);
        }
    }
}
