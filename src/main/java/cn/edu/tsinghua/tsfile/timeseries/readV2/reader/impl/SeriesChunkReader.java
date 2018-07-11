package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Jinrui Zhang
 */
public abstract class SeriesChunkReader implements TimeValuePairReader {

    protected TSDataType dataType;
    protected TSEncoding dataEncoding;
    private InputStream seriesChunkInputStream;

    private boolean pageReaderInitialized;
    private PageReader pageReader;
    private UnCompressor unCompressor;
    private TSEncoding defaultTimestampEncoding;
    private boolean hasCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;


    public SeriesChunkReader(InputStream seriesChunkInputStream, TSDataType dataType, CompressionType compressionTypeName, TSEncoding dataEncoding) {
        this.seriesChunkInputStream = seriesChunkInputStream;
        this.dataType = dataType;
        this.dataEncoding=dataEncoding;
        this.unCompressor = UnCompressor.getUnCompressor(compressionTypeName);
        this.pageReaderInitialized = false;
        defaultTimestampEncoding = TSEncoding.TS_2DIFF;
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
                Decoder valueDecoder = Decoder.getDecoderByType(dataEncoding, dataType);
                //TODO: How to get defaultTimeDecoder by TSConfig rather than hard code here ?
                Decoder defaultTimeDecoder = Decoder.getDecoderByType(defaultTimestampEncoding, TSDataType.INT64);
                pageReader = constructPageReaderForNextPage(pageHeader.getCompressedSize(), valueDecoder, defaultTimeDecoder);
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

    private PageReader constructPageReaderForNextPage(int compressedPageBodyLength, Decoder valueDecoder, Decoder timeDecoder)
            throws IOException {
        byte[] compressedPageBody = new byte[compressedPageBodyLength];
        int readLength = seriesChunkInputStream.read(compressedPageBody, 0, compressedPageBodyLength);//TODO 这里已经全部读取到内存中了
        if (readLength != compressedPageBodyLength) {
            throw new IOException("unexpected byte read length when read compressedPageBody. Expected:"
                    + compressedPageBody + ". Actual:" + readLength);
        }
        PageReader pageReader = new PageReader(new ByteArrayInputStream(unCompressor.uncompress(compressedPageBody)),
                dataType, valueDecoder, timeDecoder);
        return pageReader;
    }

    private PageHeader getNextPageHeader() throws IOException {
        return PageHeader.deserializeFrom(seriesChunkInputStream, dataType);
    }

    @Override
    public void skipCurrentTimeValuePair() {

    }

    @Override
    public void close() throws IOException {

    }
}
