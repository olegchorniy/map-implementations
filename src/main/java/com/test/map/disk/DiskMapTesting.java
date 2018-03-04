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

        DiskHahMap map = new DiskHahMap(data, fsm, 1);

        int num = 20;

        for (int i = 0; i < num; i++) {
            map.put("key - " + i, "value - " + i);
        }

        System.out.println("FSM dump");
        Dumper.dumpToStdout(fsm.getArrayCopy());
        System.out.println();

        System.out.println("Data dump");
        Dumper.dumpToStdout(data.getArrayCopy());
        System.out.println();

        for (int i = 0; i < num - 1; i++) {
            System.out.println(map.get("key - " + i));
        }

        System.out.println(map.get("key - " + (num - 1)));

        map.put("key - 0", "value - ZZZZZZZ123"); // to force displacement of this item to the last page
        System.out.println(map.get("key - 0"));

        Dumper.dumpToStdout(data.getArrayCopy());
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
