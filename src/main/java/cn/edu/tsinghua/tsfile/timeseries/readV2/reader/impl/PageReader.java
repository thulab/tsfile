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

    // decoder for value column
    private Decoder valueDecoder;

    // decoder for time column
    private Decoder timeDecoder;

    // time column in memory
    private InputStream timestampInputStream;

    // value column in memory
    private InputStream valueInputStream;

    /**
     *
     * @param pageContent uncompressed bytes size of time column, time column, value column
     * @param dataType value data type
     * @param valueDecoder decoder for value column
     * @param timeDecoder decoder for time column
     * @throws IOException exception in IO
     */
    public PageReader(InputStream pageContent, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder) throws IOException {
        this.dataType = dataType;
        this.valueDecoder = valueDecoder;
        this.timeDecoder = timeDecoder;
        splitInputStreamToTimeStampAndValue(pageContent);
    }

    /**
     * splite pageContent into two stream: time and value
     * @param pageContent uncompressed bytes size of time column, time column, value column
     * @throws IOException exception in reading data from pageContent
     */
    private void splitInputStreamToTimeStampAndValue(InputStream pageContent) throws IOException {

        int timeInputStreamLength = ReadWriteStreamUtils.readUnsignedVarInt(pageContent);

        // buffer to store uncompressed time column
        byte[] buf = new byte[timeInputStreamLength];
        int readSize = pageContent.read(buf, 0, timeInputStreamLength);
        if (readSize != timeInputStreamLength) {
            throw new IOException("Error when read bytes of encoded timestamps. " +
                    "Expect byte size : " + timeInputStreamLength + ". Read size : " + readSize);
        }
        this.timestampInputStream = new ByteArrayInputStream(buf);

        // the left uncompressed values in stream
        this.valueInputStream = pageContent;
    }

    @Override
    public boolean hasNext() throws IOException {
        return timeDecoder.hasNext(timestampInputStream) && valueDecoder.hasNext(valueInputStream);
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            long timestamp = timeDecoder.readLong(timestampInputStream);
            TsPrimitiveType value = readOneValue();
            return new TimeValuePair(timestamp, value);
        } else {
            throw new IOException("No more TimeValuePair in current page");
        }
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

    // read one value according to data type
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
            case ENUMS:
                return new TsInt(valueDecoder.readInt(valueInputStream));
            default:
                break;
        }
        throw new UnSupportedDataTypeException("Unsupported data type :" + dataType);
    }
}
