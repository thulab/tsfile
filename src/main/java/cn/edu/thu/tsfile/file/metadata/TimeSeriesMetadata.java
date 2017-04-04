package cn.edu.thu.tsfile.file.metadata;

import java.util.List;

import cn.edu.thu.tsfile.file.metadata.converter.IConverter;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSFreqType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.format.DataType;
import cn.edu.thu.tsfile.format.FreqType;
import cn.edu.thu.tsfile.format.TimeSeries;


/**
 * @Description For more information, see TimeSeries in tsfile-format
 * @author XuYi xuyi556677@163.com
 * @date Apr 29, 2016 9:43:02 PM
 */
public class TimeSeriesMetadata implements IConverter<TimeSeries> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesMetadata.class);
  
  private String measurementUID;

  /**
   * which schema/group does the delta object belongs to
   */
  private String deltaObjectType;
  
  private TSDataType type;

  /**
   * If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values. Otherwise, if
   * specified, this is the maximum bit length to store any of the values. (e.g. a low cardinality
   * INT timeseries could have this set to 32). Note that this is in the schema, and therefore fixed
   * for the entire file.
   */
  private int typeLength;
  
  private TSFreqType freqType;
  private List<Integer> frequencies;

  /**
   * If values for data consist of enum values, metadata will store all possible values in time
   * series
   */
  private List<String> enumValues;

  public TimeSeriesMetadata() {}

  public TimeSeriesMetadata(String measurementUID, TSDataType dataType, String deltaObjectType) {
    this.measurementUID = measurementUID;
    this.type = dataType;
    this.deltaObjectType = deltaObjectType;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.corp.delta.tsfile.file.metadata.converter.IConverter#convertToThrift()
   */
  @Override
  public TimeSeries convertToThrift() {
    try {
      TimeSeries timeSeriesInThrift = new TimeSeries(measurementUID,
          type == null ? null : DataType.valueOf(type.toString()), deltaObjectType);
      timeSeriesInThrift.setType_length(typeLength);
      timeSeriesInThrift.setFreq_type(freqType == null ? null : FreqType.valueOf(freqType.toString()));
      timeSeriesInThrift.setFrequencies(frequencies);
      timeSeriesInThrift.setEnum_values(enumValues);
      return timeSeriesInThrift;
    } catch (Exception e) {
      if (LOGGER.isErrorEnabled())
        LOGGER.error(
            "tsfile-file TimeSeriesMetadata: failed to convert TimeSeriesMetadata from TSFile to thrift, content is {}",
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
  public void convertToTSF(TimeSeries timeSeriesInThrift) {
    try {
      measurementUID = timeSeriesInThrift.getMeasurement_uid();
      type = timeSeriesInThrift.getType() == null ? null
          : TSDataType.valueOf(timeSeriesInThrift.getType().toString());
      deltaObjectType = timeSeriesInThrift.getDelta_object_type();
      typeLength = timeSeriesInThrift.getType_length();
      freqType = timeSeriesInThrift.getFreq_type() == null ? null
          : TSFreqType.valueOf(timeSeriesInThrift.getFreq_type().toString());
      frequencies = timeSeriesInThrift.getFrequencies();
      enumValues = timeSeriesInThrift.getEnum_values();
    } catch (Exception e) {
      if (LOGGER.isErrorEnabled())
        LOGGER.error(
            "tsfile-file TimeSeriesMetadata: failed to convert TimeSeriesMetadata from TSFile to thrift, content is {}",
            timeSeriesInThrift, e);
    }
  }

  public String getMeasurementUID() {
    return measurementUID;
  }

  public String getDeltaObjectType() {
    return deltaObjectType;
  }

  public int getTypeLength() {
    return typeLength;
  }

  public TSDataType getType() {
    return type;
  }

  public TSFreqType getFreqType() {
    return freqType;
  }

  public List<Integer> getFrequencies() {
    return frequencies;
  }

  public List<String> getEnumValues() {
    return enumValues;
  }

  @Override
  public String toString() {
    return String.format(
        "TimeSeriesMetadata: measurementUID %s, type ength %d, DataType %s, FreqType %s,frequencies %s",
        measurementUID, typeLength, type, freqType, frequencies);
  }

  public void setTypeLength(int typeLength) {
    this.typeLength = typeLength;
  }

  public void setType(TSDataType type) {
    this.type = type;
  }

  public void setFreqType(TSFreqType freqType) {
    this.freqType = freqType;
  }

  public void setFrequencies(List<Integer> frequencies) {
    this.frequencies = frequencies;
  }


  public void setMeasurementUID(String measurementUID) {
    this.measurementUID = measurementUID;
  }

  public void setDeltaObjectType(String deltaObjectType) {
    this.deltaObjectType = deltaObjectType;
  }

  public void setEnumValues(List<String> enumValues) {
    this.enumValues = enumValues;
  }
}
