package com.test.map.disk;

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Arrays;

import static com.test.map.disk.Utils.assertState;

public class DiskHahMap {

    private static final int PAGE_SIZE = 4096;

    private final SeekableByteChannel dataChannel;
    private FreeSpaceMap fsm;

    private Metadata metadata;

    public DiskHahMap(Path filesDirectory, String mapName) throws IOException {
        this(
                Utils.openRWChannel(filesDirectory.resolve(mapName + "_data")),
                Utils.openRWChannel(filesDirectory.resolve(mapName + "_fsm"))
        );
    }

    public DiskHahMap(SeekableByteChannel dataChannel, SeekableByteChannel fsmChannel) throws IOException {
        this.dataChannel = dataChannel;
        this.fsm = new FreeSpaceMap(fsmChannel);

        checkFileSize();
        initMetadata();
    }

    private void checkFileSize() throws IOException {
        long fileSize = this.dataChannel.size();

        // File should either be empty or consist of metadata page + whole number of pages
        assertState(fileSize == 0 || (fileSize - Metadata.SIZE) % PAGE_SIZE == 0, "File has invalid size: " + fileSize);
    }

    /* -------------------- Metadata management methods -------------------- */

    private void initMetadata() throws IOException {
        long fileSize = this.dataChannel.size();

        if (fileSize == 0) {
            this.metadata = Metadata.empty();
            return;
        }

        byte[] metadataBytes = Utils.read(this.dataChannel, 0, Metadata.SIZE);
        this.metadata = new Metadata(metadataBytes);
    }

    private void syncMetadata() throws IOException {
        Utils.write(this.dataChannel, 0, this.metadata.getBytes());
    }

    /* -------------------- Main API Methods -------------------- */

    public byte[] get(byte[] key) throws IOException {
        assertState(key != null, "Null key are not allowed");
        assertState(key.length <= Item.MAX_KEY_VALUE_SIZE, "Keys larger than page size are not supported for now");

        int hash = hash(key);
        int pageNumber = bucketPageNumber(bucketIndex(hash));

        Page page = readPage(pageNumber);

        while (page != null) {
            for (Item item : page.items) {
                if (item.keyEqualsTo(key, hash)) {
                    return item.value;
                }
            }

            page = readPage(page.header.nextPageNumber);
        }

        return null;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        assertState(key != null, "Null keys are not allowed");
        assertState(value != null, "Null values are not allowed");

        int hash = hash(key);
        Item newItem = new Item(hash, key, value);

        assertState(newItem.size() <= Item.MAX_SIZE, "key-value pair is too large to fit into a single page");

        // TODO: Concerns:
        // TODO:  1) We need to initialize headers of all the created pages, not only the page we add the item to.

        int bucketIndex = bucketIndex(hash);
        int pageNumber = bucketPageNumber(bucketIndex);

        /*if (pageNumber >= numPages()) {
            // grow the file and init page headers
        }

        Node<K, V>[] segment = getOrCreateSegment(segmentIndex);
        Node<K, V> node = segment[bucketIndex];

        if (node == null) {
            segment[bucketIndex] = new Node<>(key, hash, value);

            this.size++;
            split();

            return null;
        }

        Node<K, V> prev;
        do {
            if (node.keyEqualsTo(key, hash)) {
                V oldValue = node.value;
                node.value = value;

                return oldValue;
            }

            prev = node;
            node = node.next;
        } while (node != null);

        prev.next = new Node<>(key, hash, value);

        this.size++;
        split();*/
    }

    public void remove(byte[] key) {
        throw new UnsupportedOperationException();
    }

    /* -------------------- Calculations -------------------- */

    private static int hash(byte[] key) {
        return Arrays.hashCode(key);
    }

    private int bucketIndex(int hash) {
        Metadata metadata = this.metadata;

        int fullIndex = hash & mask(metadata.hashBits);
        int halfIndex = fullIndex & ~(1 << (metadata.hashBits - 1));

        return halfIndex < metadata.splitIndex ? fullIndex : halfIndex;
    }

    private int bucketPageNumber(int bucketIndex) {
        if (bucketIndex == 0) {
            return 0;
        }

        Metadata metadata = this.metadata;

        // highest bit of the bucketIndex shows how many values from the metadata.overflowPages we need to sum
        int highestBit = 31 - Integer.numberOfLeadingZeros(bucketIndex);

        int overflowPagesNumber = 0;
        for (int i = highestBit; i >= 0; i--) {
            overflowPagesNumber += metadata.overflowPages[i];
        }

        return bucketIndex + overflowPagesNumber;
    }

    private static int mask(int nBits) {
        return (1 << nBits) - 1;
    }

    /* -------------------- IO -------------------- */

    private int numPages() throws IOException {
        return (int) ((this.dataChannel.size() - Metadata.SIZE) / PAGE_SIZE);
    }

    private Page readPage(int pageNum) throws IOException {
        if (pageNum == PageHeader.NO_PAGE || pageNum >= numPages()) {
            return null;
        }

        byte[] pageBytes = Utils.read(this.dataChannel, pageOffset(pageNum), PAGE_SIZE);
        return new Page(pageBytes);
    }

    private void writePage(int pageNumber, Page page) throws IOException {
        // TODO: there should be no gaps in the file, so does it make sense to check the pageNumber here?
        Utils.write(this.dataChannel, pageOffset(pageNumber), page.getBytes());
    }

    private static long pageOffset(int pageNum) {
        return Metadata.SIZE + pageNum * PAGE_SIZE;
    }

    /* -------------- Data classes -------------- */

    @AllArgsConstructor
    private static class Metadata {

        static final int SIZE = 1 + 4 + 32 * 4;

        // 1 byte
        int hashBits;
        // 4 bytes
        int splitIndex;
        // 32 * 4
        int[] overflowPages;

        Metadata(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            this.hashBits = Byte.toUnsignedInt(buffer.get());
            this.splitIndex = buffer.getInt();
            this.overflowPages = new int[32];

            for (int i = 0; i < this.overflowPages.length; i++) {
                this.overflowPages[i] = buffer.getInt();
            }
        }

        byte[] getBytes() {
            byte[] bytes = new byte[SIZE];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            buffer.put((byte) this.splitIndex);
            buffer.putInt(this.splitIndex);

            for (int count : this.overflowPages) {
                buffer.putInt(count);
            }

            return bytes;
        }

        static Metadata empty() {
            return new Metadata(1, 0, new int[32]);
        }
    }

    @AllArgsConstructor
    private static class Page {

        PageHeader header;
        Item[] items;

        Page(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            this.header = new PageHeader(buffer);
            this.items = new Item[this.header.itemsCount];

            for (int i = 0; i < this.items.length; i++) {
                this.items[i] = new Item(buffer);
            }
        }

        byte[] getBytes() {
            byte[] bytes = new byte[PAGE_SIZE];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            buffer.put(this.header.getBytes());
            for (Item item : this.items) {
                buffer.put(item.getBytes());
            }

            return bytes;
        }
    }

    @AllArgsConstructor
    private static class PageHeader {

        static final int NO_PAGE = -1;
        static final int SIZE = 4 + 2 + 4;

        // 4 bytes
        int itemsCount;
        // 2 bytes
        int freeSpace;
        // 4 bytes
        int nextPageNumber;

        PageHeader(ByteBuffer buffer) {
            this.itemsCount = buffer.getInt();
            this.freeSpace = Short.toUnsignedInt(buffer.getShort());
            this.nextPageNumber = buffer.getInt();
        }

        byte[] getBytes() {
            byte[] bytes = new byte[SIZE];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            buffer.putInt(this.itemsCount);
            buffer.putShort((short) this.freeSpace);
            buffer.putInt(this.nextPageNumber);

            return bytes;
        }
    }

    @AllArgsConstructor
    private static class Item {
        static final int MAX_SIZE = PAGE_SIZE - PageHeader.SIZE;
        static final int MAX_KEY_VALUE_SIZE = MAX_SIZE - 4 /* hash */ - 2 /* key.length */ - 2 /* value length */;

        // 4 bytes
        int hash;
        // 2 bytes length + key byte array
        byte[] key;
        // 2 bytes length + value byte array
        byte[] value;

        Item(ByteBuffer buffer) {
            this.hash = buffer.getInt();

            int keyLength = Short.toUnsignedInt(buffer.getShort());
            int valueLength = Short.toUnsignedInt(buffer.getShort());

            this.key = new byte[keyLength];
            this.value = new byte[valueLength];

            buffer.get(this.key);
            buffer.get(this.value);
        }

        byte[] getBytes() {
            byte[] bytes = new byte[size()];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            buffer.putInt(this.hash);
            buffer.putShort((short) this.key.length);
            buffer.putShort((short) this.value.length);

            buffer.put(this.key);
            buffer.put(this.value);

            return bytes;
        }

        int size() {
            return 4 + 2 + this.key.length + 2 + this.value.length;
        }

        boolean keyEqualsTo(byte[] key, int hash) {
            return this.hash == hash && Arrays.equals(this.key, key);
        }
    }

    private static class Flags {

        private final int value;

        Flags(int value) {
            this.value = value;
        }

        Flags addFlag(Flag flag) {
            return new Flags(this.value | (1 << flag.bitNumber));
        }

        Flags clearFlag(Flag flag) {
            return new Flags(this.value & ~(1 << flag.bitNumber));
        }

        boolean hasFlag(Flag flag) {
            return (this.value & (1 << flag.bitNumber)) != 0;
        }

        int getValue() {
            return this.value;
        }
    }

    private enum Flag {
        INTERMEDIATE(0),
        PRIVATE(1);

        final int bitNumber;

        Flag(int bitNumber) {
            this.bitNumber = bitNumber;
        }
    }
}
