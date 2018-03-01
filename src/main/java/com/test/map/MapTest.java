package com.test.map;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author ochornyi
 */
public class MapTest {

    public static void main(String[] args) {
    }


    private static void mapSplitTest() {
        LinearHashMap<Integer, String> map = new LinearHashMap<>(10_000, 0.75);
//        HashMap<Integer, String> map = new HashMap<>();
//        map.printDebug();

        Random random = new Random(0x12345);
        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < 14_000; i++) {
            int n = random.nextInt(100_000);
            map.put(n, "v" + i);
            keys.add(n);
        }

        map.printDebug();

        for (Integer key : keys) {
            if (map.get(key) == null) {
                System.out.println(key + " is lost");
            }
        }

        System.out.println("Done");
    }

    private static void simpleMapTest() {
        LinearHashMap<Integer, String> map = new LinearHashMap<>();
        for (int i = 0; i < 5; i++) {
            map.put(i, "v" + i);
        }

        map.printDebug();

        map.put(2, "v2.2");
        map.printDebug();

        map.remove(0);
        map.remove(2);
        map.remove(4);
        map.printDebug();

        map.put(6, "x6");
        map.printDebug();
    }

    private static void chainLength() {
        /*
        int maxLength = -1;
        for (HashMap.Node node : ((HashMap) map).table) {
            int length = 0;
            while (node != null) {
                length++;
                node = node.next;
            }

            if (length > maxLength) {
                maxLength = length;
            }
        }

        maxLength;
        */
    }
}
