package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * For more information, see TimeSeries in cn.edu.thu.tsfile.format package
 */
public class TimeSeriesMetadata {

    private String measurementUID;

    private TSDataType type;

    public TimeSeriesMetadata() {
    }

    public TimeSeriesMetadata(String measurementUID, TSDataType dataType) {
        this.measurementUID = measurementUID;
        this.type = dataType;
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    public void setMeasurementUID(String measurementUID) {
        this.measurementUID = measurementUID;
    }

    public TSDataType getType() {
        return type;
    }

    public void setType(TSDataType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("TimeSeriesMetadata: measurementUID %s, DataType %s", measurementUID, type);
    }

    public int serialize(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, outputStream);
        if(measurementUID != null)byteLen += ReadWriteToBytesUtils.write(measurementUID, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(type, outputStream);
        if(type != null)byteLen += ReadWriteToBytesUtils.write(type, outputStream);

        return byteLen;
    }

    public int serialize(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, buffer);
        if(measurementUID != null)byteLen += ReadWriteToBytesUtils.write(measurementUID, buffer);

        byteLen += ReadWriteToBytesUtils.writeIsNull(type, buffer);
        if(type != null)byteLen += ReadWriteToBytesUtils.write(type, buffer);

        return byteLen;
    }

    public static TimeSeriesMetadata deserialize(InputStream inputStream) throws IOException {
        TimeSeriesMetadata timeSeriesMetadata = new TimeSeriesMetadata();

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            timeSeriesMetadata.measurementUID = ReadWriteToBytesUtils.readString(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            timeSeriesMetadata.type = ReadWriteToBytesUtils.readDataType(inputStream);

        return timeSeriesMetadata;
    }

    public static TimeSeriesMetadata deserialize(ByteBuffer buffer) throws IOException {
        TimeSeriesMetadata timeSeriesMetadata = new TimeSeriesMetadata();

        if(ReadWriteToBytesUtils.readIsNull(buffer))
            timeSeriesMetadata.measurementUID = ReadWriteToBytesUtils.readString(buffer);

        if(ReadWriteToBytesUtils.readIsNull(buffer))
            timeSeriesMetadata.type = ReadWriteToBytesUtils.readDataType(buffer);

        return timeSeriesMetadata;
    }
}
