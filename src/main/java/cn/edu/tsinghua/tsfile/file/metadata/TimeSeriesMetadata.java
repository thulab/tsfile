package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

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

        byteLen += ReadWriteIOUtils.writeIsNull(measurementUID, outputStream);
        if(measurementUID != null)byteLen += ReadWriteIOUtils.write(measurementUID, outputStream);

        byteLen += ReadWriteIOUtils.writeIsNull(type, outputStream);
        if(type != null)byteLen += ReadWriteIOUtils.write(type, outputStream);

        return byteLen;
    }

    public int serialize(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.writeIsNull(measurementUID, buffer);
        if(measurementUID != null)byteLen += ReadWriteIOUtils.write(measurementUID, buffer);

        byteLen += ReadWriteIOUtils.writeIsNull(type, buffer);
        if(type != null)byteLen += ReadWriteIOUtils.write(type, buffer);

        return byteLen;
    }

    public static TimeSeriesMetadata deserialize(InputStream inputStream) throws IOException {
        TimeSeriesMetadata timeSeriesMetadata = new TimeSeriesMetadata();

        if(ReadWriteIOUtils.readIsNull(inputStream))
            timeSeriesMetadata.measurementUID = ReadWriteIOUtils.readString(inputStream);

        if(ReadWriteIOUtils.readIsNull(inputStream))
            timeSeriesMetadata.type = ReadWriteIOUtils.readDataType(inputStream);

        return timeSeriesMetadata;
    }

    public static TimeSeriesMetadata deserialize(ByteBuffer buffer) throws IOException {
        TimeSeriesMetadata timeSeriesMetadata = new TimeSeriesMetadata();

        if(ReadWriteIOUtils.readIsNull(buffer))
            timeSeriesMetadata.measurementUID = ReadWriteIOUtils.readString(buffer);

        if(ReadWriteIOUtils.readIsNull(buffer))
            timeSeriesMetadata.type = ReadWriteIOUtils.readDataType(buffer);

        return timeSeriesMetadata;
    }
}
