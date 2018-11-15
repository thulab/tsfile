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
        if (timeDecoder.hasNext(timestampInputStream) && valueDecoder.hasNext(valueInputStream)) {
            cacheOneTimeValuePair();
            this.hasOneCachedTimeValuePair = true;
            return true;
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            this.hasOneCachedTimeValuePair = false;
            return cachedTimeValuePair;
        } else {
            throw new IOException("No more TimeValuePair in current page");
        }
    }

    private void cacheOneTimeValuePair() {
        long timestamp = timeDecoder.readLong(timestampInputStream);
        TsPrimitiveType value = readOneValue();

        this.cachedTimeValuePair = new TimeValuePair(timestamp, value);

        //this.cachedTimeValuePair.setTimestamp(timestamp);
        //this.cachedTimeValuePair.setValue(value);

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
                //return new TsBoolean(valueDecoder.readBoolean(valueInputStream));
            case INT32:
                intValue.setValue(valueDecoder.readInt(valueInputStream));
                return intValue;
                //return new TsInt(valueDecoder.readInt(valueInputStream));
            case INT64:
                longValue.setValue(valueDecoder.readLong(valueInputStream));
                return longValue;
                //return new TsLong(valueDecoder.readLong(valueInputStream));
            case FLOAT:
                floatValue.setValue(valueDecoder.readFloat(valueInputStream));
                return floatValue;
                //return new TsFloat(valueDecoder.readFloat(valueInputStream));
            case DOUBLE:
                doubleValue.setValue(valueDecoder.readDouble(valueInputStream));
                return doubleValue;
                //return new TsDouble(valueDecoder.readDouble(valueInputStream));
            case TEXT:
                textValue.setValue(valueDecoder.readBinary(valueInputStream));
                return textValue;
                //return new TsBinary(valueDecoder.readBinary(valueInputStream));
            case ENUMS:
                return new TsInt(valueDecoder.readInt(valueInputStream));
            default:
                break;
        }
        throw new UnSupportedDataTypeException("Unsupported data type :" + dataType);
    }
}
