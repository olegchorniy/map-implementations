package com.test.map.disk;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

import static com.test.map.disk.Utils.assertState;

public class FreeSpaceMap {

    private static final int FSM_PAGE_SIZE = 32;
    private static final byte FULL_BYTE = (byte) 0xFF;

    private final SeekableByteChannel fsmChannel;

    public FreeSpaceMap(Path fsmPath) throws IOException {
        this(Utils.openRWChannel(fsmPath));
    }

    public FreeSpaceMap(SeekableByteChannel channel) throws IOException {
        this.fsmChannel = channel;
        checkFileSize();
    }

    private void checkFileSize() throws IOException {
        assertState(fsmFileSize() % FSM_PAGE_SIZE == 0, "File consists of non integer number of pages");
    }

    public void free(int pageNum) throws IOException {
        int fsmPages = fsmPages();

        int fsmPageNum = fsmPageNum(pageNum);
        int fsmPageBitNum = fsmPageBitNum(pageNum);
        int fsmPageByte = fsmPageByte(fsmPageBitNum);
        int fsmByteBitNum = fsmByteBitNum(fsmPageBitNum);

        assertState(fsmPageNum < fsmPages, "Unallocated page can't be freed");

        int bitMask = 1 << fsmByteBitNum;
        byte[] fsmPage = readFsmPage(fsmPageNum);

        assertState((fsmPage[fsmPageByte] & bitMask) != 0, "Page is already free");

        fsmPage[fsmPageByte] &= ~bitMask;

        writeFsmPage(fsmPageNum, fsmPage);
    }

    public boolean isFree(int pageNum) throws IOException {
        int fsmPages = fsmPages();

        int fsmPageNum = fsmPageNum(pageNum);
        int fsmPageBitNum = fsmPageBitNum(pageNum);
        int fsmPageByte = fsmPageByte(fsmPageBitNum);
        int fsmByteBitNum = fsmByteBitNum(fsmPageBitNum);

        if (fsmPageNum >= fsmPages) {
            // Pages bits for which don't even allocated are considered free
            return true;
        }

        byte[] fsmPage = readFsmPage(fsmPageNum);

        return (fsmPage[fsmPageByte] & (1 << fsmByteBitNum)) == 0;
    }

    public void take(int pageNum) throws IOException {
        int fsmPages = fsmPages();

        int fsmPageNum = fsmPageNum(pageNum);
        int fsmPageBitNum = fsmPageBitNum(pageNum);
        int fsmPageByte = fsmPageByte(fsmPageBitNum);
        int fsmByteBitNum = fsmByteBitNum(fsmPageBitNum);

        int bitMask = 1 << fsmByteBitNum;
        byte[] fsmPage;

        if (fsmPageNum < fsmPages) {
            fsmPage = readFsmPage(fsmPageNum);

            // this assertion is needed only if page already exists in the file
            assertState((fsmPage[fsmPageByte] & bitMask) == 0, "Requested page isn't free");
        } else {
            // we need to initialize explicitly all the intermediary pages
            byte[] emptyPage = emptyFsmPage();

            for (int newPageNum = fsmPages; newPageNum < fsmPageNum; newPageNum++) {
                writeFsmPage(newPageNum, emptyPage);
            }

            fsmPage = emptyPage;
        }

        fsmPage[fsmPageByte] |= bitMask;
        writeFsmPage(fsmPageNum, fsmPage);
    }

    public int takeFreePage() throws IOException {
        int freePageNum = findFreePage();
        take(freePageNum);

        return freePageNum;
    }

    private int _takeFreePage() throws IOException {
        final int fsmPages = fsmPages();

        for (int pageNum = 0; pageNum < fsmPages; pageNum++) {
            byte[] page = readFsmPage(pageNum);

            for (int byteNum = 0; byteNum < page.length; byteNum++) {
                if (page[byteNum] == FULL_BYTE) {
                    continue;
                }

                int bitNum = lowestZeroBit(page[byteNum]);

                page[byteNum] |= 1 << bitNum;
                writeFsmPage(pageNum, page);

                return composePageNumber(bitNum, byteNum, pageNum);
            }
        }

        // add a new page
        byte[] newPage = emptyFsmPage();

        // set the least significant bit to 1 (to indicate that page isn't free)
        newPage[0] = 1;
        writeFsmPage(fsmPages, newPage);

        return composePageNumber(0, 0, fsmPages);
    }

    // The same as above but doesn't modify any bits and doesn't add any pages
    public int findFreePage() throws IOException {
        final int fsmPages = fsmPages();

        for (int pageNum = 0; pageNum < fsmPages; pageNum++) {
            byte[] page = readFsmPage(pageNum);

            for (int byteNum = 0; byteNum < page.length; byteNum++) {
                if (page[byteNum] == FULL_BYTE) {
                    continue;
                }

                int bitNum = lowestZeroBit(page[byteNum]);
                return composePageNumber(bitNum, byteNum, pageNum);
            }
        }

        return composePageNumber(0, 0, fsmPages);
    }

    private int fsmPages() throws IOException {
        return (int) (fsmFileSize() / FSM_PAGE_SIZE);
    }

    private long fsmFileSize() throws IOException {
        return this.fsmChannel.size();
    }

    private byte[] readFsmPage(int pageNum) throws IOException {
        return Utils.read(this.fsmChannel, fsmPageOffset(pageNum), FSM_PAGE_SIZE);
    }

    private void writeFsmPage(int pageNum, byte[] page) throws IOException {
        Utils.write(this.fsmChannel, fsmPageOffset(pageNum), page);
    }

    private static byte[] emptyFsmPage() {
        return new byte[FSM_PAGE_SIZE];
    }

    private static int fsmPageNum(int dataPageNum) {
        return dataPageNum / (8 * FSM_PAGE_SIZE);
    }

    private static int fsmPageBitNum(int dataPageNum) {
        return dataPageNum % (8 * FSM_PAGE_SIZE);
    }

    private static int fsmPageByte(int fsmPageBitNum) {
        return fsmPageBitNum / 8;
    }

    private static int fsmByteBitNum(int fsmPageBitNum) {
        return fsmPageBitNum % 8;
    }

    private static long fsmPageOffset(int pageNum) {
        return pageNum * FSM_PAGE_SIZE;
    }

    private static int composePageNumber(int bitNum, int byteNum, int pageNum) {
        return bitNum + 8 * (byteNum + FSM_PAGE_SIZE * pageNum);
    }

    public static int lowestZeroBit(byte value) {
        // In order to be able to reuse existing in JDK utility methods
        int intVal = Byte.toUnsignedInt(value);
        // Transform search of the lowest 0 bit into a search of the lowest 1 bit
        int negVal = ~intVal;

        return Integer.numberOfTrailingZeros(negVal);
    }
}
