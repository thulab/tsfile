package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * TODO this class can be taken over by TimeSeriesChunkMetadata
 * Created by zhangjinrui on 2017/12/25.
 */
public class EncodedSeriesChunkDescriptor implements SeriesChunkDescriptor {
    public static final char UUID_SPLITER = '.';
    private long offsetInFile;
    private long lengthOfBytes;
    private TSDataType dataType;
    private TsDigest valueDigest;
    private long minTimestamp;
    private long maxTimestamp;
    private long countOfPoints;
    private String measurementID;
    private long maxTombstoneTime;

    public EncodedSeriesChunkDescriptor(String measurementID, long offsetInFile, long lengthOfBytes,
                                        TSDataType dataType,
                                        TsDigest valueDigest, long minTimestamp, long maxTimestamp, long countOfPoints
    ) {
        this.measurementID = measurementID;
        this.offsetInFile = offsetInFile;
        this.lengthOfBytes = lengthOfBytes;
        this.dataType = dataType;
        this.valueDigest = valueDigest;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.countOfPoints = countOfPoints;
    }

    public boolean equals(Object object) {
        if (!(object instanceof EncodedSeriesChunkDescriptor)) {
            return false;
        }
        return getUUID().equals(((EncodedSeriesChunkDescriptor) object).getUUID());
    }

    public int hashCode() {
        return getUUID().hashCode();
    }

    private String getUUID() {
        return new StringBuilder().append(UUID_SPLITER).append(offsetInFile)
                .append(UUID_SPLITER).append(lengthOfBytes).toString();
    }


    public long getOffsetInFile() {
        return offsetInFile;
    }

    public long getLengthOfBytes() {
        return lengthOfBytes;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public TsDigest getValueDigest() {
        return valueDigest;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getCountOfPoints() {
        return countOfPoints;
    }

    public String getMeasurementID() {
        return measurementID;
    }

    public long getMaxTombstoneTime() {
        return maxTombstoneTime;
    }

    public void setMaxTombstoneTime(long maxTombstoneTime) {
        this.maxTombstoneTime = maxTombstoneTime;
    }

    @Override
    public String toString() {
        return "EncodedSeriesChunkDescriptor{" +
                ", offsetInFile=" + offsetInFile +
                ", lengthOfBytes=" + lengthOfBytes +
                ", dataType=" + dataType +
                ", valueDigest=" + valueDigest +
                ", minTimestamp=" + minTimestamp +
                ", maxTimestamp=" + maxTimestamp +
                ", countOfPoints=" + countOfPoints +
                '}';
    }

}
