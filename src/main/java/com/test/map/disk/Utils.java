package com.test.map.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

public abstract class Utils {

    public static RandomAccessFile openRW(Path file) throws IOException {
        return new RandomAccessFile(file.toFile(), "rw");
    }

    public static FileChannel openRWChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    public static void write(RandomAccessFile file, long offset, byte[] data) throws IOException {
        file.seek(offset);
        file.write(data);
    }

    public static void write(SeekableByteChannel channel, long offset, byte[] data) throws IOException {
        channel.position(offset);
        channel.write(ByteBuffer.wrap(data));
    }

    public static byte[] read(SeekableByteChannel channel, long offset, int length) throws IOException {
        byte[] result = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap(result);

        channel.position(offset);
        while (channel.read(buffer) != -1 && buffer.hasRemaining()) ;

        assertState(!buffer.hasRemaining(), "Can't read required number of bytes from the channel");

        return result;
    }

    public static byte[] read(RandomAccessFile file, long offset, int length) throws IOException {
        file.seek(offset);

        byte[] buff = new byte[length];

        int totalRead = 0;
        int lastRead;

        while ((lastRead = file.read(buff, totalRead, length - totalRead)) > 0) {
            totalRead += lastRead;
        }

        assertState(totalRead == length, () -> "Can't read " + length + "bytes from the file, EOF reached");

        return buff;
    }

    public static void assertState(boolean state, String message) {
        assertState(state, () -> message);
    }

    public static void assertState(boolean state, Supplier<String> message) {
        if (!state) {
            throw new IllegalStateException(message.get());
        }
    }
}
