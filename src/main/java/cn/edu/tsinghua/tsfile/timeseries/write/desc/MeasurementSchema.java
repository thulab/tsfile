package cn.edu.tsinghua.tsfile.timeseries.write.desc;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.encoding.encoder.Encoder;
import cn.edu.tsinghua.tsfile.encoding.encoder.TSEncodingBuilder;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * This class describes a measurement's information registered in {@linkplain FileSchema FilSchema},
 * including measurement id, data type, encoding and compressor type. For each TSEncoding,
 * MeasurementSchema maintains respective TSEncodingBuilder; For TSDataType, only ENUM has
 * TSDataTypeConverter up to now.
 *
 * @author kangrong
 * @since version 0.1.0
 */
public class MeasurementSchema implements Comparable<MeasurementSchema> {
  private static final Logger LOG = LoggerFactory.getLogger(MeasurementSchema.class);
  private TSDataType type;
  private TSEncoding encoding;
  private String measurementId;
  private TSEncodingBuilder encodingConverter;
  private Compressor compressor;
  private TSFileConfig conf;
  private Map<String, String> props = new HashMap<>();

  public MeasurementSchema() {}

  /**
   * set properties as an empty Map
   * @param measurementId measurement Id
   * @param type data type
   * @param encoding enum encoding type
   */
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding) {
    this(measurementId, type, encoding, CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().compressor), Collections.emptyMap());
  }
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType) {
    this(measurementId, type, encoding, compressionType, Collections.emptyMap());
  }

  /**
   *
   * @param measurementId   measurement Id
   * @param type            data type
   * @param encoding        enum encoding type
   * @param compressionType compression type
   * @param props           information in encoding method.
   *                        For RLE, Encoder.MAX_POINT_NUMBER
   *                        For PLAIN, Encoder.MAX_STRING_LENGTH
   */
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType,
                           Map<String, String> props) {
    this.type = type;
    this.measurementId = measurementId;
    this.encoding = encoding;
    this.props = props == null? Collections.emptyMap(): props;
    // get config from TSFileDescriptor
    this.conf = TSFileDescriptor.getInstance().getConfig();
    // initialize TSEncoding. e.g. set max error for PLA and SDT
    encodingConverter = TSEncodingBuilder.getConverter(encoding);
    encodingConverter.initFromProps(props);
    this.compressor = Compressor.getCompressor(compressionType);
  }

  public String getMeasurementId() {
    return measurementId;
  }

  public Map<String, String> getProps(){
    return props;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  public TSEncoding getEncodingType() {
    return encoding;
  }

  public TSDataType getType() {
    return type;
  }

  /**
   * return the max possible length of given type.
   *
   * @return length in unit of byte
   */
  public int getTypeLength() {
    switch (type) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 4;
      case INT64:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case TEXT:
        // 4 is the length of string in type of Integer.
        // Note that one char corresponding to 3 byte is valid only in 16-bit BMP
        return conf.maxStringLength * TSFileConfig.BYTE_SIZE_PER_CHAR + 4;
      default:
        throw new UnSupportedDataTypeException(type.toString());
    }
  }

  public Encoder getTimeEncoder() {
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    TSEncoding timeSeriesEncoder = TSEncoding.valueOf(conf.timeSeriesEncoder);
    TSDataType timeType = TSDataType.valueOf(conf.timeSeriesDataType);
    return TSEncodingBuilder.getConverter(timeSeriesEncoder).getEncoder(timeType);
  }

  /**
   * get Encoder of value from encodingConverter by measurementID and data type
   * @return Encoder for value
   */
  public Encoder getValueEncoder() {
    return encodingConverter.getEncoder(type);
  }

  public Compressor getCompressor() {
    return compressor;
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);

    byteLen += ReadWriteIOUtils.write(type, outputStream);

    byteLen += ReadWriteIOUtils.write(encoding, outputStream);

    byteLen += ReadWriteIOUtils.write(compressor.getType(), outputStream);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), outputStream);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    }

    return byteLen;
  }

  public int serializeTo(ByteBuffer buffer) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, buffer);

    byteLen += ReadWriteIOUtils.write(type, buffer);

    byteLen += ReadWriteIOUtils.write(encoding, buffer);

    byteLen += ReadWriteIOUtils.write(compressor.getType(), buffer);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, buffer);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), buffer);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), buffer);
      }
    }

    return byteLen;
  }

  public static MeasurementSchema deserializeFrom(InputStream inputStream) throws IOException {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(inputStream);

    measurementSchema.type = ReadWriteIOUtils.readDataType(inputStream);

    measurementSchema.encoding = ReadWriteIOUtils.readEncoding(inputStream);

    CompressionType compressionType = ReadWriteIOUtils.readCompressionType(inputStream);
    measurementSchema.compressor = Compressor.getCompressor(compressionType);

    int size = ReadWriteIOUtils.readInt(inputStream);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(inputStream);
        value = ReadWriteIOUtils.readString(inputStream);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  public static MeasurementSchema deserializeFrom(ByteBuffer buffer) throws IOException {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(buffer);

    measurementSchema.type = ReadWriteIOUtils.readDataType(buffer);

    measurementSchema.encoding = ReadWriteIOUtils.readEncoding(buffer);

    CompressionType compressionType = ReadWriteIOUtils.readCompressionType(buffer);
    measurementSchema.compressor = Compressor.getCompressor(compressionType);

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = ReadWriteIOUtils.readString(buffer);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MeasurementSchema that = (MeasurementSchema) o;
    return type == that.type &&
            encoding == that.encoding &&
            Objects.equals(measurementId, that.measurementId) &&
            Objects.equals(encodingConverter, that.encodingConverter) &&
            Objects.equals(compressor, that.compressor) &&
            Objects.equals(conf, that.conf) &&
            Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, encoding, measurementId, encodingConverter, compressor, conf, props);
  }

  /**
   * compare by measurementID
   */
  @Override
  public int compareTo(MeasurementSchema o) {
    if (equals(o))
      return 0;
    else
      return this.measurementId.compareTo(o.measurementId);
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    sc.addTail("[", measurementId, ",", type.toString(), ",", encoding.toString(), ",", props.toString(), ",", compressor.getType().toString());
    sc.addTail("]");
    return sc.toString();
  }
}
