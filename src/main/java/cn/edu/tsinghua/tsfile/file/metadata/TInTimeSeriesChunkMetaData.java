package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * For more information, see TimeInTimeSeriesChunkMetaData
 * in cn.edu.thu.tsfile.format package
 */
public class TInTimeSeriesChunkMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(TInTimeSeriesChunkMetaData.class);

    private TSDataType dataType;
    private long startTime;
    private long endTime;

    private TSFreqType freqType;
    private List<Integer> frequencies;

    /**
     * If values for data consist of enum values, metadata will store all possible values in time
     * series
     */
    private List<String> enumValues;

    public TInTimeSeriesChunkMetaData() {
    }

    public TInTimeSeriesChunkMetaData(TSDataType dataType, long startTime, long endTime) {
        this.dataType = dataType;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(dataType, outputStream);
        if(dataType != null)byteLen += ReadWriteToBytesUtils.write(dataType.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.write(startTime, outputStream);
        byteLen += ReadWriteToBytesUtils.write(endTime, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(freqType, outputStream);
        if(freqType != null)byteLen += ReadWriteToBytesUtils.write(freqType.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(frequencies, outputStream);
        if(frequencies != null)byteLen += ReadWriteToBytesUtils.write(frequencies, TSDataType.INT32, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(enumValues, outputStream);
        if(enumValues != null)byteLen += ReadWriteToBytesUtils.write(enumValues, TSDataType.TEXT, outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            dataType = TSDataType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        startTime = ReadWriteToBytesUtils.readLong(inputStream);
        endTime = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            freqType = TSFreqType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            frequencies = ReadWriteToBytesUtils.readIntegerList(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            enumValues = ReadWriteToBytesUtils.readStringList(inputStream);
    }

    @Override
    public String toString() {
        return String.format(
                "TInTimeSeriesChunkMetaData{ TSDataType %s, TSFreqType %s, frequencies %s, starttime %d, endtime %d, enumValues %s }",
                dataType, freqType, frequencies, startTime, endTime, enumValues);
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSFreqType getFreqType() {
        return freqType;
    }

    public void setFreqType(TSFreqType freqType) {
        this.freqType = freqType;
    }

    public List<Integer> getFrequencies() {
        return frequencies;
    }

    public void setFrequencies(List<Integer> frequencies) {
        this.frequencies = frequencies;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }
}
