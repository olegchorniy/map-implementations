package com.test.map.disk;

import crypt.ssl.utils.Dumper;
import lombok.SneakyThrows;

import java.nio.file.Paths;

public class DiskMapTesting {

    @SneakyThrows
    public static void main(String[] args) {
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
