package cn.edu.tsinghua.ts2file.timesegment.read;

import static cn.edu.tsinghua.tsfile.format.Encoding.BITMAP;
import static cn.edu.tsinghua.tsfile.format.Encoding.DELTA_BINARY_PACKED;
import static cn.edu.tsinghua.tsfile.format.Encoding.DELTA_BYTE_ARRAY;
import static cn.edu.tsinghua.tsfile.format.Encoding.DELTA_LENGTH_BYTE_ARRAY;
import static cn.edu.tsinghua.tsfile.format.Encoding.DFT;
import static cn.edu.tsinghua.tsfile.format.Encoding.DIFF;
import static cn.edu.tsinghua.tsfile.format.Encoding.GORILLA;
import static cn.edu.tsinghua.tsfile.format.Encoding.PLA;
import static cn.edu.tsinghua.tsfile.format.Encoding.PLAIN;
import static cn.edu.tsinghua.tsfile.format.Encoding.PLAIN_DICTIONARY;
import static cn.edu.tsinghua.tsfile.format.Encoding.RLE;
import static cn.edu.tsinghua.tsfile.format.Encoding.RLE_DICTIONARY;
import static cn.edu.tsinghua.tsfile.format.Encoding.SDT;
import static cn.edu.tsinghua.tsfile.format.Encoding.TS_2DIFF;

import cn.edu.tsinghua.ts2file.timesegment.filter.SegmentTimeVisitor;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.Encoding;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is mainly used to read one column of data in RowGroup.
 * It provides a number of different methods to read data
 * in different ways.
 */
public class ValueReader {

    private static final Logger LOG = LoggerFactory.getLogger(ValueReader.class);

    public Decoder decoder;
    public Decoder startTimeEncoder;
    public Decoder endTimeEncoder;
    public Decoder freqDecoder;
    public long fileOffset = -1;
    public long totalSize = -1;
    public TSDataType dataType;
    public TsDigest digest;
    public ITsRandomAccessFileReader raf;
    public List<String> enumValues;
    public CompressionTypeName compressionTypeName;
    public long rowNums;
    private long startTime, endTime;

    // save the mainFrequency of this page
    public List<float[]> mainFrequency = null;

    /**
     * @param offset    Offset for current column in file.
     * @param totalSize Total bytes size for this column.
     * @param dataType  Data type of this column
     * @param digest    Digest for this column.
     */
    public ValueReader(long offset, long totalSize, TSDataType dataType, TsDigest digest) {
        Encoding timeEncoding = getEncodingByString(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder);
        this.startTimeEncoder = Decoder.getDecoderByType(timeEncoding, TSDataType.INT64);
        this.endTimeEncoder = Decoder.getDecoderByType(timeEncoding, TSDataType.INT64);
        // this.timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        this.fileOffset = offset;
        this.totalSize = totalSize;

        this.decoder = null;
        this.dataType = dataType;
        this.digest = digest;
    }

    public ValueReader(long offset, long totalSize, TSDataType dataType, TsDigest digest, ITsRandomAccessFileReader raf,
                       CompressionTypeName compressionTypeName) {
        this(offset, totalSize, dataType, digest);
        this.compressionTypeName = compressionTypeName;
        this.raf = raf;
    }

    /**
     * @param offset              Column Offset in current file
     * @param totalSize           Total bytes size for this column
     * @param dataType            DataType for this column
     * @param digest              Digest for this column including time and value digests
     * @param raf                 RandomAccessFileReader stream
     * @param enumValues          EnumValues if this column's dataType is ENUM
     * @param compressionTypeName CompressionType used for this column
     * @param rowNums             Total of rows for this column
     */
    public ValueReader(long offset, long totalSize, TSDataType dataType, TsDigest digest, ITsRandomAccessFileReader raf,
                       List<String> enumValues, CompressionTypeName compressionTypeName, long rowNums, long startTime, long endTime) {
        this(offset, totalSize, dataType, digest, raf, compressionTypeName);
        this.enumValues = enumValues;
        this.rowNums = rowNums;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * Read time value from the page and return them.
     *
     * @param page InputStream
     * @param size time size
     * @param timeEncoder time encoder
     * @param skip If skip is true, then return long[] which is null.
     * @return common timestamp
     * @throws IOException cannot init time value
     */
    public long[] initTimeValue(InputStream page, int size, Decoder timeEncoder, boolean skip) throws IOException {
        long[] res = null;

        int length = ReadWriteStreamUtils.readUnsignedVarInt(page);
        byte[] buf = new byte[length];
        int readSize = 0;
        readSize = page.read(buf, 0, length);
        if (readSize != length) {
            throw new IOException("Expect byte size : " + length + ". Read size : " + readSize);
        }

        int idx = 0;
        if (!skip) {
            ByteArrayInputStream bis = new ByteArrayInputStream(buf);
            res = new long[size];
            while (timeEncoder.hasNext(bis)) {
                res[idx++] = timeEncoder.readLong(bis);
            }
        }

        return res;
    }

    public ByteArrayInputStream initBAIS() throws IOException {
        int length = (int) this.totalSize;
        byte[] buf = new byte[length];
        int readSize = 0;

        raf.seek(fileOffset);
        readSize = raf.read(buf, 0, length);
        if (readSize != length) {
            throw new IOException("Expect byte size : " + length + ". Read size : " + readSize);
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        return bais;
    }

    public ByteArrayInputStream initBAISForOnePage(long pageOffset) throws IOException {
        int length = (int) (this.totalSize - (pageOffset - fileOffset));
        byte[] buf = new byte[length];
        int readSize = 0;
        raf.seek(pageOffset);
        readSize = raf.read(buf, 0, length);
        if (readSize != length) {
            throw new IOException("Expect byte size : " + length + ". Read size : " + readSize);
        }

        return new ByteArrayInputStream(buf);
    }

    /**
     * //TODO what about timeFilters?
     * Judge whether current column is satisfied for given filters
     */
    public boolean columnSatisfied(SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter,
                                    SingleSeriesFilterExpression timeFilter) {
        TsDigest digest = null;
        DigestForFilter valueDigest = null;

        if (valueFilter != null) {
            digest = getDigest();
            if (getDataType() == TSDataType.ENUMS) {
                String minString = enumValues.get(BytesUtils.bytesToInt(digest.getStatistics().get(StatisticConstant.MIN_VALUE).array()) - 1);
                String maxString = enumValues.get(BytesUtils.bytesToInt(digest.getStatistics().get(StatisticConstant.MAX_VALUE).array()) - 1);
                valueDigest = new DigestForFilter(ByteBuffer.wrap(BytesUtils.StringToBytes(minString)), ByteBuffer.wrap(BytesUtils.StringToBytes(maxString)), TSDataType.TEXT);
            } else {
                valueDigest = new DigestForFilter(digest.getStatistics().get(StatisticConstant.MIN_VALUE)
                        , digest.getStatistics().get(StatisticConstant.MAX_VALUE)
                        , getDataType());
            }
        }

        DigestVisitor valueVisitor = new DigestVisitor();
        IntervalTimeVisitor timeVisitor = new IntervalTimeVisitor();
        if (valueVisitor.satisfy(valueDigest, valueFilter) && timeVisitor.satisfy(timeFilter, startTime, endTime)) {
            LOG.debug(String.format("current series is satisfy the time filter and value filter, start time : %s, end time : %s", startTime, endTime));
            return true;
        }
        return false;
    }

    /**
     * Judge whether current page is satisfied for given filters according to
     * the digests of this page
     */
    public boolean pageSatisfied(DigestForFilter timeDigestFF, DigestForFilter valueDigestFF,
                                  SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter, SingleSeriesFilterExpression freqFilter) {
        DigestVisitor digestVisitor = new DigestVisitor();
        if ((valueFilter == null && timeFilter == null)
                || (valueFilter != null && (valueDigestFF == null || digestVisitor.satisfy(valueDigestFF, valueFilter)))
                || (timeFilter != null && digestVisitor.satisfy(timeDigestFF, timeFilter))) {
            return true;
        }
        return false;
    }

    /**
     * Read the whole column without filters.
     * @param res  result
     * @param fetchSize size of result
     * @return DynamicOneColumnData
     * @throws IOException occurs error in read one column
     */
    public DynamicOneColumnData readOneColumn(DynamicOneColumnData res, int fetchSize) throws IOException {
        return readOneColumnUseFilter(res, fetchSize, null, null, null);
    }

    public SingleValueVisitor<?> getSingleValueVisitorByDataType(TSDataType type, SingleSeriesFilterExpression filter) {
        switch (type) {
            case INT32:
                return new SingleValueVisitor<Integer>(filter);
            case INT64:
                return new SingleValueVisitor<Long>(filter);
            case FLOAT:
                return new SingleValueVisitor<Float>(filter);
            case DOUBLE:
                return new SingleValueVisitor<Double>(filter);
            default:
                return SingleValueVisitorFactory.getSingleValueVisitor(type);
        }
    }

    /**
     * Read one column values with specific filters.
     * @param res result
     * @param fetchSize size of result
     * @param timeFilter  filter for time.
     * @param freqFilter  filter for frequency.
     * @param valueFilter filter for value.
     * @return answer DynamicOneColumnData
     * @throws IOException occurs error in read one column using filter
     */
    public DynamicOneColumnData readOneColumnUseFilter(DynamicOneColumnData res, int fetchSize,
                                                       SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter)
            throws IOException {

        SegmentTimeVisitor timeVisitor = new SegmentTimeVisitor();
        SingleValueVisitor<?> valueVisitor = null;
        if (valueFilter != null) {
            valueVisitor = getSingleValueVisitorByDataType(getDataType(), valueFilter);
        }

        if (res == null) {
            res = new DynamicOneColumnData(getDataType(), true, true);
            res.pageOffset = this.fileOffset;
            res.leftSize = this.totalSize;
        }

        // that res.pageOffset is -1 represents reading from the start ofcurrent column.
        if (res.pageOffset == -1) {
            res.pageOffset = this.fileOffset;
        }

        // record the length of res before reading
        int currentLength = res.valueLength;

        if (columnSatisfied(valueFilter, freqFilter, timeFilter)) {
            LOG.debug("ValueFilter satisfied Or ValueFilter is null. [ValueFilter] is: " + valueFilter);

            // Initialize the bis according to the offset in last read.
            ByteArrayInputStream bis = initBAISForOnePage(res.pageOffset);
            PageReader pageReader = new PageReader(bis, compressionTypeName);
            int pageCount = 0;
            while ((res.pageOffset - fileOffset) < totalSize && (res.valueLength - currentLength) < fetchSize) {
                int lastAvailable = bis.available();

                pageCount++;
                LOG.debug("read one page using filter, the page count is {}", pageCount);
                PageHeader pageHeader = pageReader.getNextPageHeader();

                // construct valueFilter
                Digest pageDigest = pageHeader.data_page_header.getDigest();
                DigestForFilter valueDigestFF = null;
                if (pageDigest != null) {
                    if (getDataType() == TSDataType.ENUMS) {
                        String minString = enumValues.get(BytesUtils.bytesToInt(pageDigest.getStatistics().get(StatisticConstant.MIN_VALUE).array()) - 1);
                        String maxString = enumValues.get(BytesUtils.bytesToInt(pageDigest.getStatistics().get(StatisticConstant.MAX_VALUE).array()) - 1);
                        valueDigestFF = new DigestForFilter(ByteBuffer.wrap(BytesUtils.StringToBytes(minString)), ByteBuffer.wrap(BytesUtils.StringToBytes(maxString)), TSDataType.TEXT);
                    } else {
                        valueDigestFF = new DigestForFilter(pageDigest.getStatistics().get(StatisticConstant.MIN_VALUE)
                                                            ,   pageDigest.getStatistics().get(StatisticConstant.MAX_VALUE),
                                                                getDataType());
                    }
                }

                // construct timeFilter
                long mint = pageHeader.data_page_header.min_timestamp;
                long maxt = pageHeader.data_page_header.max_timestamp;
                DigestForFilter timeDigestFF = new DigestForFilter(mint, maxt);

                if (pageSatisfied(timeDigestFF, valueDigestFF, timeFilter, valueFilter, freqFilter)) {

                    LOG.debug("page " + pageCount + " satisfied filter");

                    InputStream page = pageReader.getNextPage();

                    setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), getDataType()));

                    // get timevalues in this page
                    long[] startTimeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, startTimeEncoder, false);
                    long[] endTimeValues = initTimeValue(page, pageHeader.data_page_header.num_rows, endTimeEncoder, false);

                    try {
                        int timeIdx = 0;
                        switch (dataType) {
                            case BOOLEAN:
                                while (decoder.hasNext(page)) {
                                    boolean v = decoder.readBoolean(page);
                                    if ((timeFilter == null || timeVisitor.satisfy(timeFilter, startTimeValues[timeIdx], endTimeValues[timeIdx])) &&
                                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                                        res.putBoolean(v);
                                        res.putTime(startTimeValues[timeIdx]);
                                        res.putEmptyTime(endTimeValues[timeIdx]);
                                    }
                                    timeIdx++;
                                }
                                break;
                            case INT32:
                                while (decoder.hasNext(page)) {
                                    int v = decoder.readInt(page);
                                    if ((timeFilter == null || timeVisitor.satisfy(timeFilter, startTimeValues[timeIdx], endTimeValues[timeIdx])) &&
                                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                                        res.putInt(v);
                                        res.putTime(startTimeValues[timeIdx]);
                                        res.putEmptyTime(endTimeValues[timeIdx]);
                                    }
                                    timeIdx++;
                                }
                                break;
                            case INT64:
                                while (decoder.hasNext(page)) {
                                    long v = decoder.readLong(page);
                                    if ((timeFilter == null || timeVisitor.satisfy(timeFilter, startTimeValues[timeIdx], endTimeValues[timeIdx])) &&
                                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                                        res.putLong(v);
                                        res.putTime(startTimeValues[timeIdx]);
                                        res.putEmptyTime(endTimeValues[timeIdx]);
                                    }
                                    timeIdx++;
                                }
                                break;
                            case FLOAT:
                                while (decoder.hasNext(page)) {
                                    float v = decoder.readFloat(page);
                                    if ((timeFilter == null || timeVisitor.satisfy(timeFilter, startTimeValues[timeIdx], endTimeValues[timeIdx])) &&
                                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                                        res.putFloat(v);
                                        res.putTime(startTimeValues[timeIdx]);
                                        res.putEmptyTime(endTimeValues[timeIdx]);
                                    }
                                    timeIdx++;
                                }
                                break;
                            case DOUBLE:
                                while (decoder.hasNext(page)) {
                                    double v = decoder.readDouble(page);
                                    if ((timeFilter == null || timeVisitor.satisfy(timeFilter, startTimeValues[timeIdx], endTimeValues[timeIdx])) &&
                                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                                        res.putDouble(v);
                                        res.putTime(startTimeValues[timeIdx]);
                                        res.putEmptyTime(endTimeValues[timeIdx]);
                                    }
                                    timeIdx++;
                                }
                                break;
                            case TEXT:
                                while (decoder.hasNext(page)) {
                                    Binary v = decoder.readBinary(page);
                                    if ((timeFilter == null || timeVisitor.satisfy(timeFilter, startTimeValues[timeIdx], endTimeValues[timeIdx])) &&
                                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                                        res.putBinary(v);
                                        res.putTime(startTimeValues[timeIdx]);
                                        res.putEmptyTime(endTimeValues[timeIdx]);
                                    }
                                    timeIdx++;
                                }
                                break;
                            case ENUMS:
                                while (decoder.hasNext(page)) {
                                    int v = decoder.readInt(page) - 1;
                                    if ((timeFilter == null || timeVisitor.satisfy(timeFilter, startTimeValues[timeIdx], endTimeValues[timeIdx])) &&
                                            (valueFilter == null || valueVisitor.satisfyObject(v, valueFilter))) {
                                        res.putBinary(Binary.valueOf(enumValues.get(v)));
                                        res.putTime(startTimeValues[timeIdx]);
                                        res.putEmptyTime(endTimeValues[timeIdx]);
                                    }
                                    timeIdx++;
                                }
                                break;
                            default:
                                throw new IOException("Data type not supported. " + dataType);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else {
                    pageReader.skipCurrentPage();
                }
                res.pageOffset += (lastAvailable - bis.available());
            }

            // Represents current Column has been read all, prepare for next column in another RowGroup.
            if ((res.pageOffset - fileOffset) >= totalSize) {
                res.plusRowGroupIndexAndInitPageOffset();
            }
            return res;
        }
        return res;
    }


    public void setDecoder(Decoder d) {
        this.decoder = d;
    }

    public long getFileOffset() {
        return this.fileOffset;
    }

    public void setFileOffset(long offset) {
        this.fileOffset = offset;
    }

    public long getTotalSize() {
        return this.totalSize;
    }

    public TsDigest getDigest() {
        return this.digest;
    }

    public TSDataType getDataType() {
        return this.dataType;
    }

    public List<float[]> getMainFrequency() {
        return mainFrequency;
    }

    public void setMainFrequency(List<float[]> mainFrequency) {
        this.mainFrequency = mainFrequency;
    }

    public long getNumRows() {
        return rowNums;
    }

    public void setNumRows(long rowNums) {
        this.rowNums = rowNums;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    private Encoding getEncodingByString(String encoding) {
        switch (encoding) {
            case "PLAIN":
                return PLAIN;
            case "PLAIN_DICTIONARY":
                return PLAIN_DICTIONARY;
            case "RLE":
                return RLE;
            case "DELTA_BINARY_PACKED":
                return DELTA_BINARY_PACKED;
            case "DELTA_LENGTH_BYTE_ARRAY":
                return DELTA_LENGTH_BYTE_ARRAY;
            case "DELTA_BYTE_ARRAY":
                return DELTA_BYTE_ARRAY;
            case "RLE_DICTIONARY":
                return RLE_DICTIONARY;
            case "DIFF":
                return DIFF;
            case "TS_2DIFF":
                return TS_2DIFF;
            case "BITMAP":
                return BITMAP;
            case "PLA":
                return PLA;
            case "SDT":
                return SDT;
            case "DFT":
                return DFT;
            case "GORILLA":
                return GORILLA;
            default:
                return null;
        }
    }
}
