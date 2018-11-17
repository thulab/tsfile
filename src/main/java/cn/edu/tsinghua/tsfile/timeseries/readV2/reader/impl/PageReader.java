package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType.*;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jinrui Zhang
 */
public class PageReader implements TimeValuePairReader {

    private TSDataType dataType;
    private Decoder valueDecoder;
    private Decoder timeDecoder;
    private InputStream timestampInputStream;
    private InputStream valueInputStream;
    private boolean hasOneCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;

    private TsBoolean booleanValue = new TsBoolean(false);
    private TsInt intValue = new TsInt(0);
    private TsLong longValue = new TsLong(0);
    private TsFloat floatValue = new TsFloat(0);
    private TsDouble doubleValue = new TsDouble(0);
    private TsBinary textValue = new TsBinary(null);

    private List<Long> timeList = new ArrayList<>();
    private List<TsBoolean> booleanList = new ArrayList<>();
    private List<TsInt> intList = new ArrayList<>();
    private List<TsLong> longList = new ArrayList<>();
    private List<TsFloat> floatList = new ArrayList<>();
    private List<TsDouble> doubleList = new ArrayList<>();
    private List<TsBinary> binaryList = new ArrayList<>();
    private List<TsInt> enumsList = new ArrayList<>();

    // whether this page input stream has been deserialized to value
    private boolean initFlag = false;
    // the number of this page value list
    private int valueSize = 0;
    // the used value index of this page value list
    private int valueIndex = 0;

    public PageReader(InputStream pageContent, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder) throws IOException {
        this.dataType = dataType;
        this.valueDecoder = valueDecoder;
        this.timeDecoder = timeDecoder;
        hasOneCachedTimeValuePair = false;
        cachedTimeValuePair = new TimeValuePair(-1L, null);
        splitInputStreamToTimeStampAndValue(pageContent);
    }

    private void splitInputStreamToTimeStampAndValue(InputStream pageContent) throws IOException {
        int timeInputStreamLength = ReadWriteStreamUtils.readUnsignedVarInt(pageContent);
        byte[] buf = new byte[timeInputStreamLength];
        int readSize = pageContent.read(buf, 0, timeInputStreamLength);
        if (readSize != timeInputStreamLength) {
            throw new IOException("Error when read bytes of encoded timestamps. " +
                    "Expect byte size : " + timeInputStreamLength + ". Read size : " + readSize);
        }
        this.timestampInputStream = new ByteArrayInputStream(buf);
        this.valueInputStream = pageContent;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasOneCachedTimeValuePair) {
            return true;
        }
        if (!initFlag) {
            initPageValue();
            initFlag = true;
        }

        if (valueIndex < valueSize) {
            this.cachedTimeValuePair.setTimestamp(timeList.get(valueIndex));
            this.cachedTimeValuePair.setValue(getCacheValue());
            hasOneCachedTimeValuePair = true;
            return true;
        }

        return false;
    }

    private void initPageValue() throws IOException {
        while (timeDecoder.hasNext(timestampInputStream) && valueDecoder.hasNext(valueInputStream)) {
            valueSize ++;
            timeList.add(timeDecoder.readLong(timestampInputStream));
            switch (dataType) {
                case BOOLEAN:
                    booleanList.add(new TsBoolean(valueDecoder.readBoolean(valueInputStream)));
                    break;
                case INT32:
                    intList.add(new TsInt(valueDecoder.readInt(valueInputStream)));
                    break;
                case INT64:
                    longList.add(new TsLong(valueDecoder.readLong(valueInputStream)));
                    break;
                case FLOAT:
                    floatList.add(new TsFloat(valueDecoder.readFloat(valueInputStream)));
                    break;
                case DOUBLE:
                    doubleList.add(new TsDouble(valueDecoder.readDouble(valueInputStream)));
                    break;
                case TEXT:
                    binaryList.add(new TsBinary(valueDecoder.readBinary(valueInputStream)));
                    break;
                case ENUMS:
                    enumsList.add(new TsInt(valueDecoder.readInt(valueInputStream)));
                    break;
                default:
                    break;
            }
        }

    }

    private TsPrimitiveType getCacheValue() {
        switch (dataType) {
            case BOOLEAN:
                return booleanList.get(valueIndex);
            case INT32:
                return intList.get(valueIndex);
            case INT64:
                return longList.get(valueIndex);
            case FLOAT:
                return floatList.get(valueIndex);
            case DOUBLE:
                return doubleList.get(valueIndex);
            case TEXT:
                return binaryList.get(valueIndex);
            case ENUMS:
                return enumsList.get(valueIndex);
            default:
                throw new UnSupportedDataTypeException("Unsupported data type :" + dataType);
        }
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            this.hasOneCachedTimeValuePair = false;
            valueIndex ++;
            return cachedTimeValuePair;
        } else {
            throw new IOException("No more TimeValuePair in current page");
        }
    }

    private void cacheOneTimeValuePair() {
        long timestamp = timeDecoder.readLong(timestampInputStream);
        TsPrimitiveType value = readOneValue();

        //this.cachedTimeValuePair = new TimeValuePair(timestamp, value);

        this.cachedTimeValuePair.setTimestamp(timestamp);
        this.cachedTimeValuePair.setValue(value);

    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        timestampInputStream.close();
        valueInputStream.close();
    }

    private TsPrimitiveType readOneValue() {
        switch (dataType) {
            case BOOLEAN:
                booleanValue.setValue(valueDecoder.readBoolean(valueInputStream));
                return booleanValue;
            case INT32:
                intValue.setValue(valueDecoder.readInt(valueInputStream));
                return intValue;
            case INT64:
                longValue.setValue(valueDecoder.readLong(valueInputStream));
                return longValue;
            case FLOAT:
                floatValue.setValue(valueDecoder.readFloat(valueInputStream));
                return floatValue;
            case DOUBLE:
                doubleValue.setValue(valueDecoder.readDouble(valueInputStream));
                return doubleValue;
            case TEXT:
                textValue.setValue(valueDecoder.readBinary(valueInputStream));
                return textValue;
            case ENUMS:
                return new TsInt(valueDecoder.readInt(valueInputStream));
            default:
                break;
        }
        throw new UnSupportedDataTypeException("Unsupported data type :" + dataType);
    }
}
