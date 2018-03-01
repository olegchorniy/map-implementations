package com.test.map.disk;

import lombok.AllArgsConstructor;

import java.io.IOException;
import java.lang.reflect.Array;
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
        int pageNum = bucketPageNumber(bucketIndex);

        if (this.fsm.isFree(pageNum)) {
            // page is either exist in the file but isn't initialized or isn't even allocated
            Page newPage = Page.empty();

            newPage.items = array(newItem);
            newPage.freeSpace -= itemSize;

            writePage(pageNum, newPage);
            this.fsm.take(pageNum);

            return;
        }

        /*
            Possible cases:
            1)  Given key doesn't exist in the map. We need a page which has enough space to accommodate
                new key-value pair. We can keep track of such pages during iteration through them.
                If there was no such page we have to add a new one. We need to update FSM, map's overflowPages counter
                and link it to the last page we have checked.

            2)  Given key already exists in the map. Sub-cases:
                2.1)  New value fit in the same page. Just replace the value, update freeSpace field
                      and flush the page to the storage.
                2.2)  It doesn't. We need to remove entire item from the page where it resides
                      and put the key and the new value into another page (like in the first case).
                      Such page couldn't become empty because in such case the new item must fit in it - contradiction.
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

            if (!freePageLookingMode) {
                Item[] items = page.items;

                for (int i = 0; i < items.length; i++) {
                    Item item = items[i];

                    if (item.keyEqualsTo(key, hash)) {
                        // Free space in the page if we would have removed the old item and added the new one
                        int newFreeSpace = page.freeSpace + item.size() - itemSize;
                        if (newFreeSpace >= 0) {
                            // Case 2.1
                            page.freeSpace = newFreeSpace;
                            items[i] = newItem;

                            writePage(pageNum, page);
                            return;
                        } else {
                            // Case 2.2

                            page.freeSpace -= item.size();
                            page.items = remove(items, i);

                            writePage(pageNum, page);

                            freePageLookingMode = true;
                            break;
                        }
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
            freePage.freeSpace -= itemSize;
            freePage.items = add(freePage.items, newItem);

            writePage(freePageNum, freePage);

            return;
        }

        int newPageNum = this.fsm.findFreePage();

        Page newPage = Page.empty();
        newPage.freeSpace -= itemSize;
        newPage.items = array(newItem);

        prevPage.nextPageNumber = newPageNum;

        // And finally, IO ops
        writePage(prevPageNum, prevPage);
        writePage(newPageNum, newPage);

        this.fsm.take(newPageNum);

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

    public static <T> T[] add(T[] array, T element) {
        T[] newArray = Arrays.copyOf(array, array.length + 1);
        newArray[array.length] = element;

        return newArray;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] remove(T[] array, int index) {
        T[] newArray = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);

        System.arraycopy(array, 0, newArray, 0, index);
        System.arraycopy(array, index + 1, newArray, index, array.length - index - 1);

        return newArray;
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
