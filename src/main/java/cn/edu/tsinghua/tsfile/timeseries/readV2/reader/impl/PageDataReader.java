package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.ByteBufferBackedInputStream;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteForEncodingUtils;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType.*;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author Jinrui Zhang
 */
public class PageDataReader implements TimeValuePairReader {

    private TSDataType dataType;
    private Decoder valueDecoder;
    private Decoder timeDecoder;
    private InputStream timestampInputStream;//FIXME change to bytebuffer
    private InputStream valueInputStream;//FIXME change to bytebuffer
    private boolean hasOneCachedTimeValuePair;
    private TimeValuePair cachedTimeValuePair;


    public PageDataReader(ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder) throws IOException {
        this.dataType = dataType;
        this.valueDecoder = valueDecoder;
        this.timeDecoder = timeDecoder;
        hasOneCachedTimeValuePair = false;
        splitDataToTimeStampAndValue(pageData);
    }

    public PageDataReader(InputStream pageContent, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder) throws IOException {
        this.dataType = dataType;
        this.valueDecoder = valueDecoder;
        this.timeDecoder = timeDecoder;
        hasOneCachedTimeValuePair = false;
        splitInputStreamToTimeStampAndValue(pageContent);
    }

    private void splitInputStreamToTimeStampAndValue(InputStream pageContent) throws IOException {
        int timeInputStreamLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageContent);
        byte[] buf = new byte[timeInputStreamLength];
        int readSize = pageContent.read(buf, 0, timeInputStreamLength);//TODO 这里已经把数据读到内存中了..
        if (readSize != timeInputStreamLength) {
            throw new IOException("Error when read bytes of encoded timestamps. " +
                    "Expect byte size : " + timeInputStreamLength + ". Read size : " + readSize);
        }
        this.timestampInputStream = new ByteArrayInputStream(buf);
        this.valueInputStream = pageContent;
    }

    private void splitDataToTimeStampAndValue(ByteBuffer pageData) throws IOException {
        int timeInputStreamLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);
        ByteBuffer timeDataBuffer= pageData.slice();
        timeDataBuffer.limit(timeInputStreamLength);
        timestampInputStream= new ByteBufferBackedInputStream(timeDataBuffer);

        ByteBuffer valueDataBuffer= pageData.slice();
        valueDataBuffer.position(timeInputStreamLength);
        valueInputStream = new ByteBufferBackedInputStream(valueDataBuffer);

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
                return new TsBoolean(valueDecoder.readBoolean(valueInputStream));
            case INT32:
                return new TsInt(valueDecoder.readInt(valueInputStream));
            case INT64:
                return new TsLong(valueDecoder.readLong(valueInputStream));
            case FLOAT:
                return new TsFloat(valueDecoder.readFloat(valueInputStream));
            case DOUBLE:
                return new TsDouble(valueDecoder.readDouble(valueInputStream));
            case TEXT:
                return new TsBinary(valueDecoder.readBinary(valueInputStream));
            default:
                break;
        }
        throw new UnSupportedDataTypeException("Unsupported data type :" + dataType);
    }


}
