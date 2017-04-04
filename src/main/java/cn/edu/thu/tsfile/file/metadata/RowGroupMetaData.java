package cn.edu.thu.tsfile.file.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cn.edu.thu.tsfile.file.metadata.converter.IConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description For more information, see RowGroupMetaData in
 *              tsfile-format
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 9:57:59 PM
 */
public class RowGroupMetaData implements IConverter<cn.edu.thu.tsfile.format.RowGroupMetaData> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowGroupMetaData.class);

  private String deltaObjectUID;

  /**
   * Number of rows in this row group
   */
  private long numOfRows;

  /**
   * Total byte size of all the uncompressed time series data in this row group
   */
  private long totalByteSize;

  /**
   * This path is relative to the current file.
   */
  private String path;

  private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;

  /**
   * which schema/group does the delta object belongs to
   */
  private String deltaObjectType;

  public RowGroupMetaData() {
    timeSeriesChunkMetaDataList = new ArrayList<TimeSeriesChunkMetaData>();
  }

  public RowGroupMetaData(String deltaObjectUID, long numOfRows, long totalByteSize,
      List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList, String deltaObjectType) {
    this.deltaObjectUID = deltaObjectUID;
    this.numOfRows = numOfRows;
    this.totalByteSize = totalByteSize;
    this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
    this.deltaObjectType = deltaObjectType;
  }

  /**
   * @Description Add time series chunk metadata to list. THREAD NOT SAFE
   * @param series - time series group to add
   * @return void
   */
  public void addTimeSeriesChunkMetaData(TimeSeriesChunkMetaData metadata) {
    if (timeSeriesChunkMetaDataList == null) {
      timeSeriesChunkMetaDataList = new ArrayList<TimeSeriesChunkMetaData>();
    }
    timeSeriesChunkMetaDataList.add(metadata);
  }

  public List<TimeSeriesChunkMetaData> getMetaDatas() {
    return timeSeriesChunkMetaDataList == null ? null
        : Collections.unmodifiableList(timeSeriesChunkMetaDataList);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.corp.delta.tsfile.file.metadata.converter.IConverter#convertToThrift()
   */
  @Override
  public cn.edu.thu.tsfile.format.RowGroupMetaData convertToThrift() {
    try {
      List<cn.edu.thu.tsfile.format.TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInThrift = null;
      if (timeSeriesChunkMetaDataList != null) {
        timeSeriesChunkMetaDataListInThrift = new ArrayList<>();
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
          timeSeriesChunkMetaDataListInThrift.add(timeSeriesChunkMetaData.convertToThrift());
        }
      }
      cn.edu.thu.tsfile.format.RowGroupMetaData metaDataInThrift =
          new cn.edu.thu.tsfile.format.RowGroupMetaData(timeSeriesChunkMetaDataListInThrift,
              deltaObjectUID, totalByteSize, numOfRows, deltaObjectType);
      metaDataInThrift.setFile_path(path);
      return metaDataInThrift;
    } catch (Exception e) {
      if (LOGGER.isErrorEnabled())
        LOGGER.error(
            "tsfile-file RowGroupMetaData: failed to convert row group metadata from TSFile to thrift, row group metadata:{}",
            this, e);
      throw e;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.corp.delta.tsfile.file.metadata.converter.IConverter#convertToTSF(java.lang.Object)
   */
  @Override
  public void convertToTSF(cn.edu.thu.tsfile.format.RowGroupMetaData metaDataInThrift) {
    try {
      deltaObjectUID = metaDataInThrift.getDelta_object_uid();
      numOfRows = metaDataInThrift.getMax_num_rows();
      totalByteSize = metaDataInThrift.getTotal_byte_size();
      path = metaDataInThrift.getFile_path();
      deltaObjectType = metaDataInThrift.getDelta_object_type();
      List<cn.edu.thu.tsfile.format.TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInThrift = metaDataInThrift.getTsc_metadata();
      if (timeSeriesChunkMetaDataListInThrift == null) {
        timeSeriesChunkMetaDataList = null;
      } else {
        if (timeSeriesChunkMetaDataList == null) {
          timeSeriesChunkMetaDataList = new ArrayList<>();
        }
        timeSeriesChunkMetaDataList.clear();
        for (cn.edu.thu.tsfile.format.TimeSeriesChunkMetaData timeSeriesChunkMetaDataInThrift : timeSeriesChunkMetaDataListInThrift) {
          TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();
          timeSeriesChunkMetaData.convertToTSF(timeSeriesChunkMetaDataInThrift);
          timeSeriesChunkMetaDataList.add(timeSeriesChunkMetaData);
        }
      }
    } catch (Exception e) {
      if (LOGGER.isErrorEnabled())
        LOGGER.error(
            "tsfile-file RowGroupMetaData: failed to convert row group metadata from thrift to TSFile, row group metadata:{}",
            metaDataInThrift, e);
      throw e;
    }
  }

  @Override
  public String toString() {
    return String.format(
        "RowGroupMetaData{ delta object uid: %s, number of rows: %d, total byte size: %d, time series chunk list: %s }",
        deltaObjectUID, numOfRows, totalByteSize, timeSeriesChunkMetaDataList);
  }

  public long getNumOfRows() {
    return numOfRows;
  }

  public void setNumOfRows(long numOfRows) {
    this.numOfRows = numOfRows;
  }

  public long getTotalByteSize() {
    return totalByteSize;
  }

  public void setTotalByteSize(long totalByteSize) {
    this.totalByteSize = totalByteSize;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getDeltaObjectUID() {
    return deltaObjectUID;
  }

  public void setDeltaObjectUID(String deltaObjectUID) {
    this.deltaObjectUID = deltaObjectUID;
  }

  public List<TimeSeriesChunkMetaData> getTimeSeriesChunkMetaDataList() {
    return timeSeriesChunkMetaDataList;
  }

  public void setTimeSeriesChunkMetaDataList(
      List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
    this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
  }

  public String getDeltaObjectType() {
    return deltaObjectType;
  }

  public void setDeltaObjectType(String deltaObjectType) {
    this.deltaObjectType = deltaObjectType;
  }
}
