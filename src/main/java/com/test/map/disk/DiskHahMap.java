package com.test.map.disk;

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Arrays;

import static com.test.map.disk.Utils.assertState;

public class DiskHahMap {

    private static final int PAGE_SIZE = 256;

    private final SeekableByteChannel dataChannel;

    /**
     * Meanings of the bits corresponding to bucket pages and to overflow pages are different:
     * <ul>
     * <li>1 for an overflow page means that it's taken and 0 means that it's free.</li>
     * <li>1 for a bucket page means that it's header is already initialized and 0 means that it isn't</li>
     * </ul>
     */
    private final FreeSpaceMap fsm;

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
            setNewMetadata(Metadata.empty());
            return;
        }

        byte[] metadataBytes = Utils.read(this.dataChannel, 0, Metadata.SIZE);
        this.metadata = new Metadata(metadataBytes);
    }

    private void setNewMetadata(Metadata metadata) throws IOException {
        this.metadata = metadata;
        syncMetadata();
    }

    private void syncMetadata() throws IOException {
        Utils.write(this.dataChannel, 0, this.metadata.getBytes());
    }

    /* -------------------- Main API Methods -------------------- */

    public byte[] get(byte[] key) throws IOException {
        assertState(key != null, "Null key are not allowed");
        assertState(key.length <= Item.MAX_KEY_VALUE_SIZE, "Keys larger than page size are not supported for now");

        int hash = hash(key);
        int pageNum = bucketPageNumber(bucketIndex(hash));

        if (this.fsm.isFree(pageNum)) {
            return null;
        }

        do {
            Page page = readPage(pageNum);

            for (Item item : page.items) {
                if (item.keyEqualsTo(key, hash)) {
                    return item.value;
                }
            }

            pageNum = page.nextPageNumber;
        } while (pageNum != Page.NO_PAGE);

        return null;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        assertState(key != null, "Null keys are not allowed");
        assertState(value != null, "Null values are not allowed");

        int hash = hash(key);
        Item newItem = new Item(hash, key, value);
        int itemSize = newItem.size();

        assertState(itemSize <= Item.MAX_SIZE, "key-value pair is too large to fit into a single page");

        int bucketIndex = bucketIndex(hash);
        int pageNumber = bucketPageNumber(bucketIndex);

        if (this.fsm.isFree(pageNumber)) {
            // page is either exist in the file but isn't initialized or isn't even allocated
            Page newPage = Page.empty();

            newPage.items = array(newItem);
            newPage.freeSpace -= itemSize;

            // TODO: thing to think about: what is the correct order of write
            // TODO: operations required to preserve storage integrity and consistency
            this.fsm.take(pageNumber);
            writePage(pageNumber, newPage);

            return;
        }

        //Page bucketPage = readPage(pageNumber);
        throw new UnsupportedOperationException();

        //split();
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
        assertState(pageNum >= 0 && pageNum < numPages(), "Can't read not existing page");

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

    /* -------------------- Array manipulation routines -------------------- */

    @SafeVarargs
    private static <T> T[] array(T... items) {
        return items;
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

            buffer.put((byte) this.hashBits);
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

        static final int NO_PAGE = -1;
        static final Item[] NO_ITEMS = new Item[0];

        static final int HEADER_SIZE = 4 + 2 + 2 /* 2 additional bytes for items count in binary representation */;

        // 2 bytes
        int freeSpace;

        // 4 bytes
        int nextPageNumber;

        Item[] items;

        Page(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            int itemsCount = Short.toUnsignedInt(buffer.getShort());
            this.freeSpace = Short.toUnsignedInt(buffer.getShort());
            this.nextPageNumber = buffer.getInt();

            this.items = new Item[itemsCount];

            for (int i = 0; i < this.items.length; i++) {
                this.items[i] = new Item(buffer);
            }
        }

        byte[] getBytes() {
            byte[] bytes = new byte[PAGE_SIZE];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            buffer.putShort((short) this.items.length);
            buffer.putShort((short) this.freeSpace);
            buffer.putInt(this.nextPageNumber);

            for (Item item : this.items) {
                buffer.put(item.getBytes());
            }

            return bytes;
        }

        static Page empty() {
            // max free space amount is equal to the max size of a single item
            return new Page(Item.MAX_SIZE, NO_PAGE, NO_ITEMS);
        }
    }

    @AllArgsConstructor
    private static class Item {
        static final int MAX_SIZE = PAGE_SIZE - Page.HEADER_SIZE;
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
