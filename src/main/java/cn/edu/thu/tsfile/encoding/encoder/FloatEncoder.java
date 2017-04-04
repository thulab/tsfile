package cn.edu.thu.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.thu.tsfile.encoding.common.EndianType;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;


/**
 * @Description Encoder for float or double value using rle or two diff according to following
 *              grammar:
 *
 *              <pre>
 * {@code
 * float encoder: <maxPointvalue> <encoded-data>
 * maxPointvalue := number for accuracy of decimal places, store as unsigned var int
 * encoded-data := same as encoder's pattern
 * }
 *              </pre>
 * 
 * @author XuYi xuyi556677@163.com
 * @date May 11, 2016 11:45:03 PM
 *
 */
public class FloatEncoder extends Encoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(FloatEncoder.class);
  private Encoder encoder;

  /**
   * number for accuracy of decimal places
   */
  private int maxPointNumber;

  /**
   * maxPointValue = 10^(maxPointNumber)
   */
  private BigDecimal maxPointValue;

  /**
   * flag to check whether maxPointNumber is saved in stream
   */
  private boolean isMaxPointNumberSaved;

  public FloatEncoder(TSEncoding encodingType, TSDataType dataType, int maxPointNumber) {
    super(encodingType);
    this.maxPointNumber = maxPointNumber;
    calculateMaxPonitNum();
    isMaxPointNumberSaved = false;
    if (encodingType == TSEncoding.RLE) {
      if (dataType == TSDataType.FLOAT) {
        encoder = new IntRleEncoder(EndianType.LITTLE_ENDIAN);
        LOGGER.debug("tsfile-encoding FloatEncoder: init encoder using int-rle and float");
      } else if (dataType == TSDataType.DOUBLE || dataType == TSDataType.BIGDECIMAL) {
        encoder = new LongRleEncoder(EndianType.LITTLE_ENDIAN);
        LOGGER.debug("tsfile-encoding FloatEncoder: init encoder using long-rle and double");
      }
    } else if (encodingType == TSEncoding.TS_2DIFF) {
      if (dataType == TSDataType.FLOAT) {
        encoder = new DeltaBinaryEncoder.IntDeltaEncoder();
        LOGGER.debug("tsfile-encoding FloatEncoder: init encoder using int-delta and float");
      } else if (dataType == TSDataType.DOUBLE || dataType == TSDataType.BIGDECIMAL) {
        encoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        LOGGER.debug("tsfile-encoding FloatEncoder: init encoder using long-delta and double");
      }
    }
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) throws IOException {
    saveMaxPointNumber(out);
    int valueInt = convertFloatToInt(value);
    encoder.encode(valueInt, out);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) throws IOException {
    saveMaxPointNumber(out);
    long valueLong = convertDoubleToLong(value);
    encoder.encode(valueLong, out);
  }

  @Override
  public void encode(BigDecimal value, ByteArrayOutputStream out) throws IOException {
    saveMaxPointNumber(out);
    long valueLong = value.multiply(maxPointValue).longValue();
    encoder.encode(valueLong, out);
  }

  private void calculateMaxPonitNum() {
    if (maxPointNumber <= 0) {
      maxPointNumber = 0;
      maxPointValue = new BigDecimal(1);
    } else {
      StringBuilder builder = new StringBuilder();
      builder.append("1");
      for (int i = 0; i < maxPointNumber; i++) {
        builder.append("0");
      }
      maxPointValue = new BigDecimal(builder.toString());
    }
  }

  private int convertFloatToInt(float value) {
    BigDecimal result = maxPointValue.multiply(new BigDecimal(value));
    return Math.round(result.floatValue());
  }

  private long convertDoubleToLong(double value) {
    BigDecimal result = maxPointValue.multiply(new BigDecimal(value));
    return result.longValue();
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    encoder.flush(out);
    reset();
  }

  private void reset() {
    isMaxPointNumberSaved = false;
  }

  private void saveMaxPointNumber(ByteArrayOutputStream out) throws IOException {
    if (!isMaxPointNumberSaved) {
      ReadWriteStreamUtils.writeUnsignedVarInt(maxPointNumber, out);
      isMaxPointNumberSaved = true;
    }
  }

  @Override
  public int getOneItemMaxSize(){
    return encoder.getOneItemMaxSize();
  }

  @Override
  public long getMaxByteSize(){
    return encoder.getMaxByteSize();
  }

}
