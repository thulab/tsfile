package cn.edu.thu.tsfile.file.metadata;

import java.util.List;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.file.metadata.converter.IConverter;
import cn.edu.thu.tsfile.format.DataType;
import cn.edu.thu.tsfile.format.ValueInTimeSeriesChunkMetaData;

/**
 * @Description For more information, see
 *              ValueInTimeSeriesChunkMetaData in tsfile-format
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 10:22:07 PM
 */
public class VInTimeSeriesChunkMetaData implements IConverter<ValueInTimeSeriesChunkMetaData> {
  private static final Logger LOGGER = LoggerFactory.getLogger(VInTimeSeriesChunkMetaData.class);

  private TSDataType dataType;

  private TSDigest digest;
  private int maxError;

  /**
   * If values for data consist of enum values, metadata will store all possible values in time
   * series
   */
  private List<String> enumValues;

  public VInTimeSeriesChunkMetaData() {}

  public VInTimeSeriesChunkMetaData(TSDataType dataType) {
    this.dataType = dataType;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.corp.delta.tsfile.file.metadata.converter.IConverter#convertToThrift()
   */
  @Override
  public ValueInTimeSeriesChunkMetaData convertToThrift() {
    try {
      ValueInTimeSeriesChunkMetaData vTimeSeriesChunkMetaDataInThrift = new ValueInTimeSeriesChunkMetaData(
          dataType == null ? null : DataType.valueOf(dataType.toString()));
      vTimeSeriesChunkMetaDataInThrift.setMax_error(maxError);
      vTimeSeriesChunkMetaDataInThrift.setEnum_values(enumValues);
      vTimeSeriesChunkMetaDataInThrift.setDigest(digest == null ? null : digest.convertToThrift());
      return vTimeSeriesChunkMetaDataInThrift;
    } catch (Exception e) {
      if (LOGGER.isErrorEnabled())
        LOGGER.error(
            "tsfile-file VInTimeSeriesChunkMetaData: failed to convert ValueInTimeSeriesChunkMetaData from TSFile to thrift, content is {}",
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
  public void convertToTSF(ValueInTimeSeriesChunkMetaData vTimeSeriesChunkMetaDataInThrift) {
    try {
      this.dataType = vTimeSeriesChunkMetaDataInThrift.getData_type() == null ? null : TSDataType.valueOf(vTimeSeriesChunkMetaDataInThrift.getData_type().toString());
      this.maxError = vTimeSeriesChunkMetaDataInThrift.getMax_error();
      this.enumValues = vTimeSeriesChunkMetaDataInThrift.getEnum_values();
      if (vTimeSeriesChunkMetaDataInThrift.getDigest() == null) {
        this.digest = null;
      }else{
        this.digest = new TSDigest();
        this.digest.convertToTSF(vTimeSeriesChunkMetaDataInThrift.getDigest());
      }
    } catch (Exception e) {
      if (LOGGER.isErrorEnabled())
        LOGGER.error(
            "tsfile-file VInTimeSeriesChunkMetaData: failed to convert ValueInTimeSeriesChunkMetaData from thrift to TSFile, content is {}",
            vTimeSeriesChunkMetaDataInThrift, e);
      throw e;
    }
  }

  @Override
  public String toString() {
    return String.format("VInTimeSeriesChunkMetaData{ TSDataType %s, TSDigest %s, maxError %d, enumValues %s }", dataType, digest,
        maxError, enumValues);
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public TSDigest getDigest() {
    return digest;
  }

  public int getMaxError() {
    return maxError;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public void setDigest(TSDigest digest) {
    this.digest = digest;
  }

  public void setMaxError(int maxError) {
    this.maxError = maxError;
  }

  public List<String> getEnumValues() {
    return enumValues;
  }

  public void setEnumValues(List<String> enumValues) {
    this.enumValues = enumValues;
  }
}
