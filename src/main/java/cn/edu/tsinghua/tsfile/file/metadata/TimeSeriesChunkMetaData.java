package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
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
 *
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

    private CompressionType compression;//FIXME put me to TimeSeriesMetaData

    private long numOfPoints;

    /**
     * total byte size of all uncompressed pages in this time series chunk (including the headers)
     */
    private long totalByteSize;

    private long startTime;

    private long endTime;

    private TSDataType dataType;

    private TsDigest valuesStatistics;

    private TSEncoding dataEncoding;//FIXME put me to TimeSeriesMetaData

    private TimeSeriesChunkMetaData(){}

    public TimeSeriesChunkMetaData(String measurementUID, long fileOffset, CompressionType compression,
                                   TSDataType dataType, long startTime, long endTime, TSEncoding dataEncoding) {
        this.measurementUID = measurementUID;
        this.fileOffset = fileOffset;
        this.compression = compression;
        this.dataType = dataType;
        this.startTime = startTime;
        this.endTime = endTime;
        this.dataEncoding=dataEncoding;
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

    public TSEncoding getDataEncoding() {
        return dataEncoding;
    }

    public void setDataEncoding(TSEncoding dataEncoding) {
        this.dataEncoding = dataEncoding;
    }
    /**
     *
     * @return total byte size of all uncompressed pages in this time series chunk (including the headers)
     */
    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    /**
     * @return Byte offset of the corresponding data in the file
     */
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

    /**
     *
     * @return Byte offset of timseries chunk metadata in the file.
     *          Each timeseries chunk metadata has fixed length: 68 bytes.
     */
    public long getTsDigestOffset() {
        return tsDigestOffset;
    }

    public void setTsDigestOffset(long tsDigestOffset) {
        this.tsDigestOffset = tsDigestOffset;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        //byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, outputStream);
        byteLen += ReadWriteToBytesUtils.write(measurementUID, outputStream);

        byteLen += ReadWriteToBytesUtils.write(fileOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(tsDigestOffset, outputStream);

        //byteLen += ReadWriteToBytesUtils.writeIsNull(compression, outputStream);
        byteLen += ReadWriteToBytesUtils.write(compression, outputStream);

        byteLen += ReadWriteToBytesUtils.write(numOfPoints, outputStream);
        byteLen += ReadWriteToBytesUtils.write(totalByteSize, outputStream);
        byteLen += ReadWriteToBytesUtils.write(startTime, outputStream);
        byteLen += ReadWriteToBytesUtils.write(endTime, outputStream);

        //byteLen += ReadWriteToBytesUtils.writeIsNull(dataType, outputStream);
        byteLen += ReadWriteToBytesUtils.write(dataType, outputStream);

        byteLen += ReadWriteToBytesUtils.write(dataEncoding,outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(valuesStatistics, outputStream);
        if(valuesStatistics != null) byteLen += ReadWriteToBytesUtils.write(valuesStatistics, outputStream);


        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        //byteLen += ReadWriteToBytesUtils.writeIsNull(measurementUID, buffer);
        byteLen += ReadWriteToBytesUtils.write(measurementUID, buffer);

        byteLen += ReadWriteToBytesUtils.write(fileOffset, buffer);
        byteLen += ReadWriteToBytesUtils.write(tsDigestOffset, buffer);

        //byteLen += ReadWriteToBytesUtils.writeIsNull(compression, buffer);
        byteLen += ReadWriteToBytesUtils.write(compression, buffer);

        byteLen += ReadWriteToBytesUtils.write(numOfPoints, buffer);
        byteLen += ReadWriteToBytesUtils.write(totalByteSize, buffer);
        byteLen += ReadWriteToBytesUtils.write(startTime, buffer);
        byteLen += ReadWriteToBytesUtils.write(endTime, buffer);

        //byteLen += ReadWriteToBytesUtils.writeIsNull(dataType, buffer);
        byteLen += ReadWriteToBytesUtils.write(dataType, buffer);

        byteLen += ReadWriteToBytesUtils.write(dataEncoding, buffer);

        byteLen += ReadWriteToBytesUtils.writeIsNull(valuesStatistics, buffer);
        if(valuesStatistics != null) byteLen += ReadWriteToBytesUtils.write(valuesStatistics, buffer);

        return byteLen;
    }

    public static TimeSeriesChunkMetaData deserializeFrom(InputStream inputStream) throws IOException {
        TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();

        timeSeriesChunkMetaData.measurementUID = ReadWriteToBytesUtils.readString(inputStream);

        timeSeriesChunkMetaData.fileOffset = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.tsDigestOffset = ReadWriteToBytesUtils.readLong(inputStream);

        timeSeriesChunkMetaData.compression = ReadWriteToBytesUtils.readCompressionType(inputStream);


        timeSeriesChunkMetaData.numOfPoints = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.totalByteSize = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.startTime = ReadWriteToBytesUtils.readLong(inputStream);
        timeSeriesChunkMetaData.endTime = ReadWriteToBytesUtils.readLong(inputStream);

        timeSeriesChunkMetaData.dataType = ReadWriteToBytesUtils.readDataType(inputStream);
        timeSeriesChunkMetaData.dataEncoding = ReadWriteToBytesUtils.readEncoding(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)) {
            timeSeriesChunkMetaData.valuesStatistics = ReadWriteToBytesUtils.readDigest(inputStream);
        }

        return timeSeriesChunkMetaData;
    }

    public static TimeSeriesChunkMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
        TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();

        timeSeriesChunkMetaData.measurementUID = ReadWriteToBytesUtils.readString(buffer);

        timeSeriesChunkMetaData.fileOffset = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.tsDigestOffset = ReadWriteToBytesUtils.readLong(buffer);

        timeSeriesChunkMetaData.compression = ReadWriteToBytesUtils.readCompressionType(buffer);

        timeSeriesChunkMetaData.numOfPoints = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.totalByteSize = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.startTime = ReadWriteToBytesUtils.readLong(buffer);
        timeSeriesChunkMetaData.endTime = ReadWriteToBytesUtils.readLong(buffer);

        timeSeriesChunkMetaData.dataType = ReadWriteToBytesUtils.readDataType(buffer);

        timeSeriesChunkMetaData.dataEncoding = ReadWriteToBytesUtils.readEncoding(buffer);

        if(ReadWriteToBytesUtils.readIsNull(buffer)) {
            timeSeriesChunkMetaData.valuesStatistics = ReadWriteToBytesUtils.readDigest(buffer);
        }

        return timeSeriesChunkMetaData;
    }
}
