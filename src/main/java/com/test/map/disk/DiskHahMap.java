package com.test.map.disk;

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

import static com.test.map.disk.Utils.assertState;

/**
 * Optimizations:
 * <ul>
 * <li>Sort items within each page by their keys</li>
 * <li>Compaction during page split</li>
 * <li>Replace bucket page with the first overflow page if it becomes free upon item deletion</li>
 * </ul>
 * <p>
 * Things to think about:
 * <ul>
 * <li>Definition of {@code load factor}</li>
 * </ul>
 */
public class DiskHahMap {

    private static final int HASH_LENGTH = 32;
    private static final int PAGE_SIZE = 256;

    private final SeekableByteChannel dataChannel;
    private final FreeSpaceMap fsm;

    private Metadata metadata;

    public DiskHahMap(SeekableByteChannel dataChannel, SeekableByteChannel fsmChannel, int initialSize) throws IOException {
        this.dataChannel = dataChannel;
        this.fsm = new FreeSpaceMap(fsmChannel, true);

        this.metadata = Metadata.forInitial(initialSize);

        assertEmpty();

        initBuckets();
        writeMetadata();
    }

    public DiskHahMap(SeekableByteChannel dataChannel, SeekableByteChannel fsmChannel) throws IOException {
        this.dataChannel = dataChannel;
        this.fsm = new FreeSpaceMap(fsmChannel);

        checkFileSizeAndInit();
    }

    private void assertEmpty() throws IOException {
        assertState(this.dataChannel.size() == 0, "Data channel is not empty");
    }

    private void checkFileSizeAndInit() throws IOException {
        // File should contain at least metadata and all the pages determined by the metadata's fields

        long dataSize = this.dataChannel.size();
        assertState(dataSize >= Metadata.SIZE, "Data channel size is less than metadata size: " + dataSize);

        readMetadata();

        long exactExpectedSize = Metadata.SIZE + this.metadata.expectedNumberOfPages() * PAGE_SIZE;
        assertState(exactExpectedSize == dataSize, "Invalid data channel size: " + dataSize);
    }

    private void initBuckets() throws IOException {
        Page emptyPage = Page.empty();

        for (int i = 0; i < this.metadata.bucketsNum(); i++) {
            writePage(i, emptyPage);
        }
    }

    /* -------------------- Metadata management methods -------------------- */

    private void readMetadata() throws IOException {
        byte[] metadataBytes = Utils.read(this.dataChannel, 0, Metadata.SIZE);
        this.metadata = new Metadata(metadataBytes);
    }

    private void writeMetadata() throws IOException {
        Utils.write(this.dataChannel, 0, this.metadata.getBytes());
    }

    /* -------------------- Testing API Methods -------------------- */

    public String get(String key) throws IOException {
        byte[] value = get(key.getBytes());
        if (value == null) {
            return null;
        }

        return new String(value);
    }

    public void put(String key, String value) throws IOException {
        put(key.getBytes(), value.getBytes());
    }

    /* -------------------- Main API Methods -------------------- */

    public byte[] get(byte[] key) throws IOException {
        checkKeyNotNull(key);
        checkKeySize(key);

        final int hash = hash(key);
        final int bucketPageNum = bucketPageNumber(bucketIndex(hash));

        if (bucketPageNum >= this.metadata.bucketsNum()) {
            return null;
        }

        int pageNum = bucketPageNum;

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
        checkKeyNotNull(key);
        checkValueNotNull(value);

        int hash = hash(key);
        Item newItem = new Item(hash, key, value);
        int itemSize = newItem.size();

        assertState(itemSize <= Item.MAX_SIZE, "key-value pair is too large to fit into a single page");

        Metadata metadata = this.metadata;

        int bucketIndex = bucketIndex(hash);
        int pageNum = bucketPageNumber(bucketIndex);

        /*
         *  Possible cases:
         *  1)  Given key doesn't exist in the map. We need a page which has enough space to accommodate
         *      new key-value pair. We can keep track of such pages during iteration through them.
         *      If there was no such page we have to add a new one. We need to update FSM, map's overflowPages counter
         *      and link it to the last page we have checked.
         *
         *  2)  Given key already exists in the map. Sub-cases:
         *      2.1)  New value fit in the same page. Just replace the value, update freeSpace field
         *            and flush the page to the storage.
         *      2.2)  It doesn't. We need to remove entire item from the page where it resides
         *            and put the key and the new value into another page (like in the first case).
         *            Such page couldn't become empty because in such case the new item must fit in it - contradiction.
         */

        // previous page for the case when we have to add a new page to the chain
        int prevPageNum;
        Page prevPage;

        // page which has enough space to accommodate new item
        int freePageNum = 0;
        Page freePage = null;

        // true indicates that we have found the key in the map but we haven't found a page which has enough free space.
        // We need to keep looking for such page but don't need to check the page's items.
        boolean freePageLookingMode = false;

        do {
            Page page = readPage(pageNum);
            Item[] items = page.items;

            for (int i = 0; i < items.length && !freePageLookingMode; i++) {
                Item item = items[i];

                if (item.keyEqualsTo(key, hash)) {
                    if (page.freeSpace + item.size() >= itemSize) {
                        // Case 2.1

                        page.replace(i, newItem);
                        writePage(pageNum, page);

                        return;
                    } else {
                        // Case 2.2

                        page.removeItem(i);
                        writePage(pageNum, page);

                        freePageLookingMode = true;
                        break;
                    }
                }
            }

            if (freePage == null && itemSize <= page.freeSpace) {
                freePage = page;
                freePageNum = pageNum;
            }

            prevPage = page;
            prevPageNum = pageNum;

            pageNum = page.nextPageNumber;

            // Stop iterations when we either reached the end of the chain or we found a free page in the free-page-looking mode.
        } while (pageNum != Page.NO_PAGE && (!freePageLookingMode || freePage == null));

        // Case 1 or 2.2 (in case of lack of free space in the original page)

        if (freePage != null) {
            freePage.addItem(newItem);
            writePage(freePageNum, freePage);

            return;
        }

        // Update map's metadata increasing overflow pages counter
        // Important: this line should be executed before fsmPageNumToOverflowPageNum invocation.
        metadata.incOverflowPages();

        int newFsmPageNum = this.fsm.findFreePage();
        int newPageNum = fsmPageNumToOverflowPageNum(newFsmPageNum);

        // Create a new page and add a new item into it
        Page newPage = Page.empty();
        newPage.addItem(newItem);

        // Establish a link between the last page in the chain and the new one
        prevPage.nextPageNumber = newPageNum;

        // And finally, IO ops
        writePage(prevPageNum, prevPage);
        writePage(newPageNum, newPage);

        writeMetadata();

        this.fsm.take(newFsmPageNum);

        //split();
    }

    public void remove(byte[] key) throws IOException {
        checkKeyNotNull(key);
        checkKeySize(key);

        final int hash = hash(key);
        final int bucketPageNum = bucketPageNumber(bucketIndex(hash));

        if (bucketPageNum >= this.metadata.bucketsNum()) {
            return;
        }

        Page prevPage = null;
        int prevPageNum = -1;
        int pageNum = bucketPageNum;

        do {
            Page page = readPage(pageNum);

            for (int i = 0; i < page.items.length; i++) {
                Item item = page.items[i];

                if (item.keyEqualsTo(key, hash)) {
                    page.removeItem(i);

                    // Page isn't empty or it's a bucket page: just write it back to the channel.
                    if (page.items.length != 0 || prevPage == null) {
                        writePage(pageNum, page);
                        return;
                    }

                    // It's an overflow page. Update pointers and return this page to the FSM.
                    // We DON'T need to update the page's content in the channel, so we can save one disk IO op.
                    prevPage.nextPageNumber = page.nextPageNumber;

                    this.fsm.free(overflowPageNumToFsmPageNum(pageNum));
                    writePage(prevPageNum, prevPage);

                    return;
                }
            }

            prevPage = page;
            prevPageNum = pageNum;
            pageNum = page.nextPageNumber;
        } while (pageNum != Page.NO_PAGE);
    }

    /* -------------------- Precondition checks -------------------- */

    private static void checkKeyNotNull(byte[] key) {
        assertState(key != null, "Null keys are not allowed");
    }

    private static void checkValueNotNull(byte[] key) {
        assertState(key != null, "Null values are not allowed");
    }

    private static void checkKeySize(byte[] key) {
        assertState(key.length <= Item.MAX_KEY_VALUE_SIZE, "Keys larger than page size are not supported for now");
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

        int overflowPagesSoFar = 0;
        for (int i = highestBit; i >= 0; i--) {
            overflowPagesSoFar += metadata.overflowPages[i];
        }

        return bucketIndex + overflowPagesSoFar;
    }

    private int fsmPageNumToOverflowPageNum(int fsmPageNum) {
        final int splitPoint = this.metadata.activeSplitPoint();
        final int[] overflowPages = this.metadata.overflowPages;

        for (int i = 0, pagesCount = 0; i <= splitPoint; i++) {
            pagesCount += overflowPages[i];

            if (fsmPageNum < pagesCount) {
                return fsmPageNum + (1 << i);
            }
        }

        throw new IllegalStateException("There is no overflow page with fsm number: " + fsmPageNum);
    }

    // TODO: double-check this code!!!
    private int overflowPageNumToFsmPageNum(int overflowPageNum) {
        final int splitPoint = this.metadata.activeSplitPoint();
        final int[] overflowPages = this.metadata.overflowPages;

        for (int i = 0, pageCount = 0, buckets = 1; i <= splitPoint; i++, buckets <<= 1) {
            pageCount += overflowPages[i];

            int totalPageCount = pageCount + buckets;

            if (overflowPageNum < totalPageCount) {
                return overflowPageNum - buckets;
            }
        }

        throw new IllegalStateException("There is no overflow page with number: " + overflowPageNum);
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
        Utils.write(this.dataChannel, pageOffset(pageNumber), page.getBytes());
    }

    private static long pageOffset(int pageNum) {
        return Metadata.SIZE + pageNum * PAGE_SIZE;
    }

    /* -------------------- Array manipulation routines -------------------- */

    private static <T> T[] add(T[] array, T element) {
        T[] newArray = Arrays.copyOf(array, array.length + 1);
        newArray[array.length] = element;

        return newArray;
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] remove(T[] array, int index) {
        T[] newArray = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);

        System.arraycopy(array, 0, newArray, 0, index);
        System.arraycopy(array, index + 1, newArray, index, array.length - index - 1);

        return newArray;
    }

    /* -------------- Data classes -------------- */

    @AllArgsConstructor
    private static class Metadata {

        static final int ARRAY_LENGTH = HASH_LENGTH + 1;
        static final int SIZE = 1 + 4 + ARRAY_LENGTH * 4;

        // 1 byte
        int hashBits;
        // 4 bytes
        int splitIndex;
        // ARRAY_LENGTH * 4
        int[] overflowPages;

        Metadata(byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            this.hashBits = Byte.toUnsignedInt(buffer.get());
            this.splitIndex = buffer.getInt();
            this.overflowPages = new int[ARRAY_LENGTH];

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

        void incOverflowPages() {
            this.overflowPages[activeSplitPoint()]++;
        }

        int activeSplitPoint() {
            // If no splits were performed so far we consider current
            // split point as active or the next split point otherwise.

            int splitPoint = this.hashBits;
            if (this.splitIndex == 0) {
                splitPoint--;
            }

            return splitPoint;
        }

        int bucketsNum() {
            return (1 << (this.hashBits - 1)) + this.splitIndex;
        }

        int expectedNumberOfPages() {
            final int splitPoint = activeSplitPoint();

            int numPages = bucketsNum();
            for (int i = 0; i <= splitPoint; i++) {
                numPages += this.overflowPages[i];
            }

            return numPages;
        }

        static Metadata forInitial(int initialSize) {
            int bucketsNum = (initialSize == 1)
                    ? 1
                    : (Integer.highestOneBit(initialSize - 1) << 1);

            int hashBits = Integer.SIZE - Integer.numberOfLeadingZeros(bucketsNum);

            return new Metadata(hashBits, 0, new int[ARRAY_LENGTH]);
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

        void addItem(Item item) {
            this.freeSpace -= item.size();
            this.items = add(this.items, item);
        }

        void removeItem(int index) {
            this.freeSpace += this.items[index].size();
            this.items = remove(this.items, index);
        }

        void replace(int index, Item newItem) {
            this.freeSpace = this.freeSpace + this.items[index].size() - newItem.size();
            this.items[index] = newItem;
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


    /*public DiskHahMap(Path filesDirectory, String mapName) throws IOException {
        this(
                Utils.openRWChannel(filesDirectory.resolve(mapName + "_data")),
                Utils.openRWChannel(filesDirectory.resolve(mapName + "_fsm"))
        );
    }*/

    /* -------------------- Handy factory methods -------------------- */

    public static DiskHahMap oneBucket(SeekableByteChannel dataChannel, SeekableByteChannel fsmChannel) throws IOException {
        return new DiskHahMap(dataChannel, fsmChannel, 1);
    }
}
