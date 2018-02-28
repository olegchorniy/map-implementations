package com.test.map;

/**
 * @author ochornyi
 */
public class MapTest {

    public static void main(String[] args) {
        for (int i = 0; i < (1 << Byte.SIZE); i++) {
            byte b = (byte) i;
            int val = Byte.toUnsignedInt(b);

            System.out.println(Integer.toBinaryString(val) + ": " + (7 - (Integer.numberOfLeadingZeros(val) - 24)));
        }
    }
}
