package cn.edu.tsinghua.ts2file.timesegment.write.record;

import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.BooleanDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.DoubleDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.IntDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.StringDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.segment.ISegmentWriter;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.BigDecimalDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.EnumDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.LongDataPoint;
import java.io.IOException;
import java.math.BigDecimal;

public abstract class SegmentDataPoint {

  protected final TSDataType type;
  protected final String measurementId;

  public SegmentDataPoint(TSDataType type, String measurementId) {
    this.type = type;
    this.measurementId = measurementId;
  }

  /**
   * Construct one data point with data type and value
   *
   * @param dataType data type
   * @param measurementId measurement id
   * @param value value in string format
   * @return data point class according to data type
   */
  public static SegmentDataPoint getDataPoint(TSDataType dataType, String measurementId, String value) {
    SegmentDataPoint dataPoint = null;
    switch (dataType) {
      case INT32:
        dataPoint = new IntDataPoint(measurementId, Integer.valueOf(value));
        break;
      case INT64:
        dataPoint = new LongDataPoint(measurementId, Long.valueOf(value));
        break;
      case FLOAT:
        dataPoint = new FloatDataPoint(measurementId, Float.valueOf(value));
        break;
      case DOUBLE:
        dataPoint = new DoubleDataPoint(measurementId, Double.valueOf(value));
        break;
      case BOOLEAN:
        dataPoint = new BooleanDataPoint(measurementId, Boolean.valueOf(value));
        break;
      case TEXT:
        dataPoint = new StringDataPoint(measurementId, new Binary(value));
        break;
      case BIGDECIMAL:
        dataPoint = new BigDecimalDataPoint(measurementId, new BigDecimal(value));
        break;
      case ENUMS:
        dataPoint = new EnumDataPoint(measurementId, Integer.valueOf(value));
        break;
      default:
        throw new UnSupportedDataTypeException("This data type is not supoort -" + dataType);
    }
    return dataPoint;
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public abstract Object getValue();

  public TSDataType getType() {
    return type;
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer(" ");
    sc.addTail("{measurement id:", measurementId, "type:", type, "value:", getValue(), "}");
    return sc.toString();
  }

  public void setInteger(int value) {
    throw new UnsupportedOperationException("set Integer not support in DataPoint");
  }

  public void setLong(long value) {
    throw new UnsupportedOperationException("set Long not support in DataPoint");
  }

  public void setBoolean(boolean value) {
    throw new UnsupportedOperationException("set Boolean not support in DataPoint");
  }

  public void setFloat(float value) {
    throw new UnsupportedOperationException("set Float not support in DataPoint");
  }

  public void setDouble(double value) {
    throw new UnsupportedOperationException("set Double not support in DataPoint");
  }

  public void setString(Binary value) {
    throw new UnsupportedOperationException("set String not support in DataPoint");
  }

  public void setBigDecimal(BigDecimal value) {
    throw new UnsupportedOperationException("set BigDecimal not support in DataPoint");
  }

  public abstract void write(long startTime, long endTime, ISegmentWriter writer) throws IOException;
}
