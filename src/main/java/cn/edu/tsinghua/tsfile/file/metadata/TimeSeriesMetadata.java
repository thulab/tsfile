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
 * For more information, see TimeSeries in cn.edu.thu.tsfile.format package
 */
public class TimeSeriesMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesMetadata.class);

    private String measurementUID;

    private TSDataType type;

    /**
     * If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values. Otherwise, if
     * specified, this is the maximum bit length to store any of the values. (e.g. a low cardinality
     * INT timeseries could have this set to 32). Note that this is in the schema, and therefore fixed
     * for the entire file.
     */
    private int typeLength;

    private TSFreqType freqType;
    private List<Integer> frequencies;

    /**
     * If values for data consist of enum values, metadata will store all possible values in time
     * series
     */
    private List<String> enumValues;

    public TimeSeriesMetadata() {
    }

    public TimeSeriesMetadata(String measurementUID, TSDataType dataType) {
        this.measurementUID = measurementUID;
        this.type = dataType;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, outputStream);
        if(measurementUID != null)byteLen += ReadWriteToBytesUtils.write(measurementUID, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(type, outputStream);
        if(type != null)byteLen += ReadWriteToBytesUtils.write(type.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.write(typeLength, outputStream);

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
            measurementUID = ReadWriteToBytesUtils.readString(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            type = TSDataType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        typeLength = ReadWriteToBytesUtils.readInt(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            freqType = TSFreqType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            frequencies = ReadWriteToBytesUtils.readIntegerList(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            enumValues = ReadWriteToBytesUtils.readStringList(inputStream);
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    public void setMeasurementUID(String measurementUID) {
        this.measurementUID = measurementUID;
    }

    public int getTypeLength() {
        return typeLength;
    }

    public void setTypeLength(int typeLength) {
        this.typeLength = typeLength;
    }

    public TSDataType getType() {
        return type;
    }

    public void setType(TSDataType type) {
        this.type = type;
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

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }

    @Override
    public String toString() {
        return String.format(
                "TimeSeriesMetadata: measurementUID %s, type length %d, DataType %s, FreqType %s,frequencies %s",
                measurementUID, typeLength, type, freqType, frequencies);
    }
}
