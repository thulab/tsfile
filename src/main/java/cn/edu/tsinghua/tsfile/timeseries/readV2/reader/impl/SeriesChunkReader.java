package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author Jinrui Zhang
 */
public abstract class SeriesChunkReader implements TimeValuePairReader {

    private InputStream seriesChunkInputStream;

    private boolean pageReaderInitialized;
    private PageDataReader pageReader;
    private UnCompressor unCompressor;
//    private TSEncoding defaultTimestampEncoding;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;
     ChunkHeader chunkHeader;
    Decoder valueDecoder;
    //TODO: How to get defaultTimeDecoder by TSConfig rather than hard code here ?
    Decoder timeDecoder = Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT64);

    public SeriesChunkReader(InputStream seriesChunkInputStream) {
        this.seriesChunkInputStream = seriesChunkInputStream;
        this.pageReaderInitialized = false;
        try {
            chunkHeader=ChunkHeader.deserializeFrom(seriesChunkInputStream);
            this.unCompressor = UnCompressor.getUnCompressor(chunkHeader.getCompressionType());
            valueDecoder = Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedTimeValuePair) {
            return true;
        }
        //Judge whether next satisfied page exists
        while (true) {
            if (!pageReaderInitialized) {
                boolean hasMoreSatisfiedPage = constructPageReaderIfNextSatisfiedPageExists();
                if (!hasMoreSatisfiedPage) {
                    return false;
                }
                pageReaderInitialized = true;
            }

            while (pageReader.hasNext()) {
                TimeValuePair timeValuePair = pageReader.next();
                if (timeValuePairSatisfied(timeValuePair)) {
                    this.hasCachedTimeValuePair = true;
                    this.cachedTimeValuePair = timeValuePair;
                    return true;
                }
            }
            pageReaderInitialized = false;
        }
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair;
        }
        throw new IOException("No more timeValuePair in current MemSeriesChunk");
    }

    private boolean constructPageReaderIfNextSatisfiedPageExists() throws IOException {
        boolean gotNextPageReader = false;
        while (hasNextPageInStream() && !gotNextPageReader) {
            PageHeader pageHeader = getNextPageHeader();
            if (pageSatisfied(pageHeader)) {
                pageReader = constructPageReaderForNextPage(pageHeader.getCompressedSize());
                gotNextPageReader = true;
            } else {
                skipBytesInStreamByLength(pageHeader.getCompressedSize());
            }
        }
        return gotNextPageReader;

    }

    private boolean hasNextPageInStream() throws IOException {
        if (seriesChunkInputStream.available() > 0) {
            return true;
        }
        return false;
    }

    public abstract boolean pageSatisfied(PageHeader pageHeader);

    public abstract boolean timeValuePairSatisfied(TimeValuePair timeValuePair);

    private void skipBytesInStreamByLength(long length) throws IOException {
        seriesChunkInputStream.skip(length);
    }

    private PageDataReader constructPageReaderForNextPage(int compressedPageBodyLength)
            throws IOException {
        byte[] compressedPageBody = new byte[compressedPageBodyLength];
        int readLength = seriesChunkInputStream.read(compressedPageBody, 0, compressedPageBodyLength);//TODO 这里已经全部读取到内存中了
        if (readLength != compressedPageBodyLength) {
            throw new IOException("unexpected byte read length when read compressedPageBody. Expected:"
                    + compressedPageBody + ". Actual:" + readLength);
        }
        PageDataReader pageReader = new PageDataReader(ByteBuffer.wrap(unCompressor.uncompress(compressedPageBody)),
                chunkHeader.getDataType(), valueDecoder, timeDecoder);
        return pageReader;
    }

    private PageHeader getNextPageHeader() throws IOException {
        return PageHeader.deserializeFrom(seriesChunkInputStream, chunkHeader.getDataType());
    }

    @Override
    public void skipCurrentTimeValuePair() {

    }

    @Override
    public void close() throws IOException {

    }
}
