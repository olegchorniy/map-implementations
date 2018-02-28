package com.test.map.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

public class InMemoryChannel implements SeekableByteChannel {

    private byte[] buff;
    private int position;

    public InMemoryChannel() {
        this(0);
    }

    public InMemoryChannel(int initialSize) {
        this.buff = new byte[initialSize];
        this.position = 0;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int remaining = remaining();
        if (remaining <= 0) {
            return -1;
        }

        int bytesToCopy = Math.min(dst.remaining(), remaining);

        dst.put(this.buff, this.position, bytesToCopy);
        this.position += bytesToCopy;

        return bytesToCopy;
    }

    private int remaining() {
        return this.buff.length - this.position;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int srcRemaining = src.remaining();
        if (srcRemaining == 0) {
            // don't expand buffer if there are no bytes to be copied
            return 0;
        }

        int newPosition = this.position + srcRemaining;
        if (newPosition > this.buff.length) {
            // allocate a new buffer and copy all the data from the old one
            byte[] newBuff = new byte[newPosition];
            // we don't need to copy bytes which will be overwritten immediately
            int length = Math.min(this.position, this.buff.length);

            System.arraycopy(this.buff, 0, newBuff, 0, length);

            this.buff = newBuff;
        }

        src.get(this.buff, this.position, srcRemaining);
        this.position = newPosition;

        return srcRemaining;
    }

    @Override
    public long position() throws IOException {
        return this.position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        this.position = safeCast(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return this.buff.length;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        if (size < this.buff.length) {
            this.buff = Arrays.copyOfRange(this.buff, 0, safeCast(size));
        }

        return this;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() throws IOException {
    }

    private static int safeCast(long value) {
        if (value > Integer.MAX_VALUE) {
            throw new IllegalStateException("Value too large to fit into 4 bytes signed integer: " + value);
        }

        return (int) value;
    }

    public byte[] getArrayCopy() {
        return Arrays.copyOf(this.buff, this.buff.length);
    }
}
