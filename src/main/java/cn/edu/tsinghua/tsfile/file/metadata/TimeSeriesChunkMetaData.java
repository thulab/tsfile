package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private CompressionTypeName compression;

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

    public TimeSeriesChunkMetaData(String measurementUID, long fileOffset, CompressionTypeName compression,
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

    public CompressionTypeName getCompression() {
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
}
