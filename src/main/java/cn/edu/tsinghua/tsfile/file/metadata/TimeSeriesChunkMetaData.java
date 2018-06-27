package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * For more information, see TimeSeriesChunkMetaData in cn.edu.thu.tsfile.format package
 */
public class TimeSeriesChunkMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesChunkMetaData.class);

    private String measurementUID;

    /**
     * Byte offset of the corresponding data in the file
     */
    private long fileOffset;

    /**
     * Byte offset of timseries chunk metadata in the file
     * Each timeseries chunk metadata has fixed length: 68 bytes.
     */
    private long tsDigestOffset;

    private CompressionType compression;

    private long numOfPoints;

    /**
     * total byte size of all uncompressed pages in this time series chunk (including the headers)
     */
    private long totalByteSize;

    private long startTime;

    private long endTime;

    private TSDataType dataType;

    private TsDigest valuesStatistics;

    public TimeSeriesChunkMetaData() {
    }

    public TimeSeriesChunkMetaData(String measurementUID, long fileOffset, CompressionType compression,
                                   TSDataType dataType, long startTime, long endTime) {
        this();
        this.measurementUID = measurementUID;
        this.fileOffset = fileOffset;
        this.compression = compression;
        this.dataType = dataType;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return String.format("numPoints %d, totalByteSize %d", numOfPoints, totalByteSize);
    }

    public long getNumOfPoints() {
        return numOfPoints;
    }

    public void setNumOfPoints(long numRows) {
        this.numOfPoints = numRows;
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    public CompressionType getCompression() {
        return compression;
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {

        this.dataType = dataType;
    }

    public TsDigest getDigest() {
        return valuesStatistics;
    }

    public void setDigest(TsDigest digest) {
        this.valuesStatistics = digest;
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

    public long getTsDigestOffset() {
        return tsDigestOffset;
    }

    public void setTsDigestOffset(long tsDigestOffset) {
        this.tsDigestOffset = tsDigestOffset;
    }

    public int serialize(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, outputStream);
        if(measurementUID != null)byteLen += ReadWriteToBytesUtils.write(measurementUID, outputStream);

        byteLen += ReadWriteToBytesUtils.write(fileOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(tsDigestOffset, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(compression, outputStream);
        if(compression != null) byteLen += ReadWriteToBytesUtils.write(compression, outputStream);

        byteLen += ReadWriteToBytesUtils.write(numOfPoints, outputStream);
        byteLen += ReadWriteToBytesUtils.write(totalByteSize, outputStream);
        byteLen += ReadWriteToBytesUtils.write(startTime, outputStream);
        byteLen += ReadWriteToBytesUtils.write(endTime, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(dataType, outputStream);
        if(dataType != null) byteLen += ReadWriteToBytesUtils.write(dataType, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(valuesStatistics, outputStream);
        if(valuesStatistics != null) byteLen += ReadWriteToBytesUtils.write(valuesStatistics, outputStream);

        return byteLen;
    }

    public int serialize(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, buffer);
        if(measurementUID != null)byteLen += ReadWriteToBytesUtils.write(measurementUID, buffer);

        byteLen += ReadWriteToBytesUtils.write(fileOffset, buffer);
        byteLen += ReadWriteToBytesUtils.write(tsDigestOffset, buffer);

        byteLen += ReadWriteToBytesUtils.writeIsNull(compression, buffer);
        if(compression != null) byteLen += ReadWriteToBytesUtils.write(compression, buffer);

        byteLen += ReadWriteToBytesUtils.write(numOfPoints, buffer);
        byteLen += ReadWriteToBytesUtils.write(totalByteSize, buffer);
        byteLen += ReadWriteToBytesUtils.write(startTime, buffer);
        byteLen += ReadWriteToBytesUtils.write(endTime, buffer);

        byteLen += ReadWriteToBytesUtils.writeIsNull(dataType, buffer);
        if(dataType != null) byteLen += ReadWriteToBytesUtils.write(dataType, buffer);

        byteLen += ReadWriteToBytesUtils.writeIsNull(valuesStatistics, buffer);
        if(valuesStatistics != null) byteLen += ReadWriteToBytesUtils.write(valuesStatistics, buffer);

        return byteLen;
    }

    public static TimeSeriesChunkMetaData deserialize(InputStream inputStream) throws IOException {
        TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            timeSeriesChunkMetaData.measurementUID = ReadWriteToBytesUtils.readString(inputStream);

        timeSeriesChunkMetaData.fileOffset = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.tsDigestOffset = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            timeSeriesChunkMetaData.compression = ReadWriteToBytesUtils.readCompressionType(inputStream);
        }

        timeSeriesChunkMetaData.numOfPoints = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.totalByteSize = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.startTime = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.endTime = ReadWriteToBytesUtils.readLong(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            timeSeriesChunkMetaData.dataType = ReadWriteToBytesUtils.readDataType(inputStream);
        }

        if(ReadWriteToBytesUtils.readIsNull(inputStream)) {
            timeSeriesChunkMetaData.valuesStatistics = ReadWriteToBytesUtils.readDigest(inputStream);
        }

        return timeSeriesChunkMetaData;
    }

    public static TimeSeriesChunkMetaData deserialize(ByteBuffer buffer) throws IOException {
        TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();

        if(ReadWriteToBytesUtils.readIsNull(buffer))
            timeSeriesChunkMetaData.measurementUID = ReadWriteToBytesUtils.readString(buffer);

        timeSeriesChunkMetaData.fileOffset = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.tsDigestOffset = ReadWriteToBytesUtils.readLong(buffer);

        if(ReadWriteToBytesUtils.readIsNull(buffer)){
            timeSeriesChunkMetaData.compression = ReadWriteToBytesUtils.readCompressionType(buffer);
        }

        timeSeriesChunkMetaData.numOfPoints = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.totalByteSize = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.startTime = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.endTime = ReadWriteToBytesUtils.readLong(buffer);

        if(ReadWriteToBytesUtils.readIsNull(buffer)){
            timeSeriesChunkMetaData.dataType = ReadWriteToBytesUtils.readDataType(buffer);
        }

        if(ReadWriteToBytesUtils.readIsNull(buffer)) {
            timeSeriesChunkMetaData.valuesStatistics = ReadWriteToBytesUtils.readDigest(buffer);
        }

        return timeSeriesChunkMetaData;
    }
}
