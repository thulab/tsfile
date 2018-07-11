package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

/**
 * TODO this class can be taken over by TimeSeriesChunkMetadata
 * Created by zhangjinrui on 2017/12/25.
 */
public class EncodedSeriesChunkDescriptor implements SeriesChunkDescriptor {
    public static final char UUID_SPLITER = '.';
    private String filePath;
    private long offsetInFile;
    private long lengthOfBytes;
//    private CompressionType compressionType;
    private TSDataType dataType;
    private TsDigest valueDigest;
    private long minTimestamp;
    private long maxTimestamp;
    private long countOfPoints;
   // private TSEncoding dataEncoding;
    //private List<String> enumValueList;
    private String measurementID;

    public EncodedSeriesChunkDescriptor(String measurementID, long offsetInFile, long lengthOfBytes, //CompressionType compressionType,
                                        TSDataType dataType,
                                        TsDigest valueDigest, long minTimestamp, long maxTimestamp, long countOfPoints//, TSEncoding dataEncoding
    ) {
        this.measurementID=measurementID;
        this.offsetInFile = offsetInFile;
        this.lengthOfBytes = lengthOfBytes;
//        this.compressionType = compressionType;
        this.dataType = dataType;
        this.valueDigest = valueDigest;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.countOfPoints = countOfPoints;
//        this.dataEncoding = dataEncoding;
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
        return new StringBuilder().append(filePath).append(UUID_SPLITER).append(offsetInFile)
                .append(UUID_SPLITER).append(lengthOfBytes).toString();
    }

    public String getFilePath() {
        return filePath;
    }

    public long getOffsetInFile() {
        return offsetInFile;
    }

    public long getLengthOfBytes() {
        return lengthOfBytes;
    }

//    public CompressionType getCompressionType() {
//        return compressionType;
//    }
//
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

//    public List<String> getEnumValueList() {
//        return enumValueList;
//    }

    public String getMeasurementID() {
        return measurementID;
    }

//    public TSEncoding getDataEncoding() {
//        return dataEncoding;
//    }
//
//    public void setDataEncoding(TSEncoding dataEncoding) {
//        this.dataEncoding = dataEncoding;
//    }

    @Override
    public String toString() {
        return "EncodedSeriesChunkDescriptor{" +
                "filePath='" + filePath + '\'' +
                ", offsetInFile=" + offsetInFile +
                ", lengthOfBytes=" + lengthOfBytes +
//                ", compressionType=" + compressionType +
                ", dataType=" + dataType +
                ", valueDigest=" + valueDigest +
                ", minTimestamp=" + minTimestamp +
                ", maxTimestamp=" + maxTimestamp +
                ", countOfPoints=" + countOfPoints +
//                ", enumValueList=" + enumValueList +
//                ", dataEncoding=" + dataEncoding +
                '}';
    }
}
