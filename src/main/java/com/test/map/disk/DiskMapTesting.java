package com.test.map.disk;

import crypt.ssl.utils.Dumper;
import lombok.SneakyThrows;

import java.nio.file.Paths;

public class DiskMapTesting {

    @SneakyThrows
    public static void main(String[] args) {
        mapTesting();
    }

    @SneakyThrows
    private static void initTesting() {
        InMemoryChannel data = new InMemoryChannel();
        InMemoryChannel fsm = new InMemoryChannel();

        DiskHahMap map = DiskHahMap.oneBucket(data, fsm);
        DiskHahMap map2 = new DiskHahMap(data, fsm);

        Dumper.dumpToStdout(data.getArrayCopy());
        System.out.println();
        Dumper.dumpToStdout(fsm.getArrayCopy());
    }

    @SneakyThrows
    public static void mapTesting() {

        InMemoryChannel data = new InMemoryChannel();
        InMemoryChannel fsm = new InMemoryChannel();

        DiskHahMap map = new DiskHahMap(data, fsm, 4);

        int num = 200;

        for (int i = 0; i < num; i++) {
            map.put(key(i), "value - " + i);
        }

        System.out.println("After initial filling");
        System.out.println("FSM");
        Dumper.dumpToStdout(fsm.getArrayCopy());
        System.out.println();

        System.out.println("Data");
        Dumper.dumpToStdout(data.getArrayCopy());
        System.out.println();

        for (int i = 0; i < num; i++) {
            System.out.println(map.get(key(i)));
        }

        for (int i = 5; i < num - 5; i++) {
            map.remove(key(i));
        }

        System.out.println("After removing");
        for (int i = 0; i < num; i++) {
            System.out.println(map.get(key(i)));
        }

        Dumper.dumpToStdout(fsm.getArrayCopy());
        System.out.println();
        Dumper.dumpToStdout(data.getArrayCopy());

        for (int i = num / 4; i <= (3 * num) / 4; i++) {
            map.put(key(i), "Restored:" + i);
        }

        System.out.println("After restoring some entries");
        for (int i = 0; i < num; i++) {
            System.out.println(map.get(key(i)));
        }

        Dumper.dumpToStdout(fsm.getArrayCopy());
        System.out.println();
        Dumper.dumpToStdout(data.getArrayCopy());
    }

    private static String key(int n) {
        return "key#" + n;
    }

    @SneakyThrows
    public static void inMemFsmTest() {
        InMemoryChannel channel = new InMemoryChannel();
        FreeSpaceMap memoryMap = new FreeSpaceMap(channel);

        for (int i = 0; i < 16; i++) {
            System.out.println(memoryMap.takeFreePage());
        }

        memoryMap.free(1);
        memoryMap.free(5);
        memoryMap.free(9);

        for (int i = 0; i < 4; i++) {
            System.out.println(memoryMap.takeFreePage());
        }

        for (int i = 0; i < 4; i++) {
            memoryMap.free(i);
        }

        Dumper.dumpToStdout(channel.getArrayCopy());
        System.out.println();

        memoryMap.take(20_000);
        Dumper.dumpToStdout(channel.getArrayCopy());
    }

    @SneakyThrows
    public static void fsmTesting() {
        FreeSpaceMap fsm = new FreeSpaceMap(Paths.get("D:", "work_dir", "files_tests", "fsm_ch"));

        for (int i = 0; i < 8; i++) {
            System.out.println(fsm.takeFreePage());
        }

        fsm.free(1);
        fsm.free(6);

        System.out.println(fsm.takeFreePage());
        System.out.println(fsm.takeFreePage());
        System.out.println(fsm.takeFreePage());
    }
}
