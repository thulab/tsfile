package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.Encoding;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * @author Jinrui Zhang
 */
public abstract class SeriesChunkReader implements TimeValuePairReader {

    protected TSDataType dataType;
    private InputStream seriesChunkInputStream;

    private boolean hasUnconsumedPage;
    private PageReader pageReader;
    private UnCompressor unCompressor;
    protected boolean hasCachedTimeValuePair;
    protected TimeValuePair cachedTimeValuePair;
    private long maxTombstoneTime;


    public SeriesChunkReader(InputStream seriesChunkInputStream, TSDataType dataType, CompressionTypeName compressionTypeName) {
        this.seriesChunkInputStream = seriesChunkInputStream;
        this.dataType = dataType;
        this.unCompressor = UnCompressor.getUnCompressor(compressionTypeName);
        this.hasUnconsumedPage = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedTimeValuePair) {
            return true;
        }

        //Judge whether next satisfied page exists
        while (true) {
            if (!hasUnconsumedPage) {

                // construct next satisfied page header
                boolean hasMoreSatisfiedPage = constructPageReaderIfNextSatisfiedPageExists();

                // if there does not exist a satisfied page, return false
                if (!hasMoreSatisfiedPage) {
                    return false;
                }
                hasUnconsumedPage = true;
            }

            // check whether there exists a satisfied time value pair in current page
            while (pageReader.hasNext()) {

                // read next time value pair
                TimeValuePair timeValuePair = pageReader.next();

                // check if next time value pair satisfy the condition
                if (timeValuePairSatisfied(timeValuePair) && timeValuePair.getTimestamp() > maxTombstoneTime) {

                    // cache next satisfied time value pair
                    this.cachedTimeValuePair = timeValuePair;
                    this.hasCachedTimeValuePair = true;

                    return true;
                }
            }
            hasUnconsumedPage = false;
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

    /**
     * Read page one by one from InputStream and check the page header whether this page satisfies the filter.
     * Skip the unsatisfied pages and construct PageReader for the first page satisfied.
     * @return whether there exists a satisfied page
     * @throws IOException exception when reading page
     */
    private boolean constructPageReaderIfNextSatisfiedPageExists() throws IOException {

        boolean gotNextPageReader = false;

        while (seriesChunkInputStream.available() > 0 && !gotNextPageReader) {
            // deserialize a PageHeader from seriesChunkInputStream
            PageHeader pageHeader = ReadWriteThriftFormatUtils.readPageHeader(seriesChunkInputStream);

            // if the current page satisfies the filter
            if (pageSatisfied(pageHeader)) {
                pageReader = constructPageReader(pageHeader);
                gotNextPageReader = true;
            } else {
                // skip the current page body
                long skipped = seriesChunkInputStream.skip(pageHeader.getCompressed_page_size());
                if(skipped != pageHeader.getCompressed_page_size())
                    throw new IOException("the page body is not complete. Expected skipped body size:"
                            + pageHeader.getCompressed_page_size() + ". Actual skipped:" + skipped);
            }
        }
        return gotNextPageReader;
    }


    public abstract boolean pageSatisfied(PageHeader pageHeader);

    public abstract boolean timeValuePairSatisfied(TimeValuePair timeValuePair);


    /**
     * construct PageReader by PageHeader
     */
    private PageReader constructPageReader(PageHeader pageHeader) throws IOException{

        // get value decoder
        Decoder valueDecoder = Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType);

        //TODO: add time encoding type in PageHeader and get it from PageHeader
        Decoder timeDecoder = Decoder.getDecoderByType(Encoding.TS_2DIFF, TSDataType.INT64);

        // read compressed page body from InputStream
        byte[] compressedPageBody = new byte[pageHeader.getCompressed_page_size()];
        int readLength = seriesChunkInputStream.read(compressedPageBody);

        // check if read fully
        if (readLength != compressedPageBody.length) {
            throw new IOException("unexpected byte read length when read compressedPageBody. Expected:"
                    + Arrays.toString(compressedPageBody) + ". Actual:" + readLength);
        }
        return new PageReader(new ByteArrayInputStream(unCompressor.uncompress(compressedPageBody)),
                dataType, valueDecoder, timeDecoder);
    }


    @Override
    public void skipCurrentTimeValuePair() {

    }

    @Override
    public void close() throws IOException {

    }

    public void setMaxTombstoneTime(long maxTombStoneTime) {
        this.maxTombstoneTime = maxTombStoneTime;
    }

    public long getMaxTombstoneTime() {
        return this.maxTombstoneTime;
    }
}
