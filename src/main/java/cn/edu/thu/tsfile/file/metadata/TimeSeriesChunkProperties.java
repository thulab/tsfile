package cn.edu.thu.tsfile.file.metadata;

import cn.edu.thu.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;

/**
 * @Description store required members in TimeSeriesChunkMetaData
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 10:41:56 PM
 */
public class TimeSeriesChunkProperties {
  private String measurementUID;
  
  /** 
   * Type of this time series
   */
  @Deprecated
  private TSChunkType tsChunkType;
  
  /** 
   * Byte offset in file_path to the RowGroupMetaData 
   */
  private long fileOffset;
  private CompressionTypeName compression;

  public TimeSeriesChunkProperties() {}

  public TimeSeriesChunkProperties(String measurementUID, TSChunkType tsChunkType, long fileOffset,
      CompressionTypeName compression) {
    this.measurementUID = measurementUID;
    this.tsChunkType = tsChunkType;
    this.fileOffset = fileOffset;
    this.compression = compression;
  }

  public TSChunkType getTsChunkType() {
    return tsChunkType;
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

  @Override
  public String toString() {
    return String.format("measurementUID %s, TSChunkType %s, fileOffset %d, CompressionTypeName %s",
        measurementUID, tsChunkType, fileOffset, compression);
  }
}
