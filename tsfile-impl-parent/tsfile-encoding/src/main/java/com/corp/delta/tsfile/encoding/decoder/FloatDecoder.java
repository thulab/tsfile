package com.corp.delta.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corp.delta.tsfile.common.exception.TSFileDecodingException;
import com.corp.delta.tsfile.common.utils.Binary;
import com.corp.delta.tsfile.common.utils.ReadWriteStreamUtils;
import com.corp.delta.tsfile.encoding.common.EndianType;
import com.corp.delta.tsfile.file.metadata.enums.TSDataType;
import com.corp.delta.tsfile.file.metadata.enums.TSEncoding;


/**
 * @Description Decoder for float or double value using rle or two diff. For more info about
 *              encoding pattern, see {@link com.corp.delta.tsfile.encoding.encoder.FloatEncoder}
 * @author XuYi xuyi556677@163.com
 * @date May 11, 2016 11:45:27 PM
 *
 */
public class FloatDecoder extends Decoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(FloatDecoder.class);
  private Decoder decoder;

  /**
   * maxPointValue = 10^(maxPointNumer) maxPointNumber can be read from stream
   */
  private BigDecimal maxPointValue;

  /**
   * flag to indicate whether we have read maxPointNumber and calculate maxPointValue
   */
  private boolean isMaxPointNumberRead;

  public FloatDecoder(TSEncoding encodingType, TSDataType dataType) {
    super(encodingType);
    if (encodingType == TSEncoding.RLE) {
      if (dataType == TSDataType.FLOAT) {
        decoder = new IntRleDecoder(EndianType.LITTLE_ENDIAN);
        LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using int-rle and float");
      } else if (dataType == TSDataType.DOUBLE || dataType == TSDataType.BIGDECIMAL) {
        decoder = new LongRleDecoder(EndianType.LITTLE_ENDIAN);
        LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using long-rle and double");
      }
    } else if (encodingType == TSEncoding.TS_2DIFF) {
      if (dataType == TSDataType.FLOAT) {
        decoder = new DeltaBinaryDecoder.IntDeltaDecoder();
        LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using int-delta and float");
      } else if (dataType == TSDataType.DOUBLE || dataType == TSDataType.BIGDECIMAL) {
        decoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        LOGGER.debug("tsfile-encoding FloatDecoder: init decoder using long-delta and double");
      }
    }
    isMaxPointNumberRead = false;
  }

  @Override
  public float readFloat(InputStream in) {
    readMaxPointValue(in);
    int value = decoder.readInt(in);
    BigDecimal result = new BigDecimal(value).divide(maxPointValue);
    return result.floatValue();
  }

  @Override
  public double readDouble(InputStream in) {
    readMaxPointValue(in);
    long value = decoder.readLong(in);
    BigDecimal result = new BigDecimal(value).divide(maxPointValue);
    return result.doubleValue();
  }

  @Override
  public BigDecimal readBigDecimal(InputStream in) {
    readMaxPointValue(in);
    long value = decoder.readLong(in);
    BigDecimal valueOrigin = new BigDecimal(value);
    return valueOrigin.divide(maxPointValue);
  }

  private void readMaxPointValue(InputStream in) {
    try {
      if (!isMaxPointNumberRead) {
        int maxPointNumber = ReadWriteStreamUtils.readUnsignedVarInt(in);
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
        isMaxPointNumberRead = true;
      }
    } catch (IOException e) {
      LOGGER.error("tsfile-encoding FloatDecoder: error occurs when reading maxPointValue", e);
    }
  }

  @Override
  public boolean hasNext(InputStream in) throws IOException {
    if (decoder == null) {
      return false;
    }
    return decoder.hasNext(in);
  }

  @Override
  public Binary readBinary(InputStream in) {
    throw new TSFileDecodingException("Method readBinary is not supproted by FloatDecoder");
  }

  @Override
  public boolean readBoolean(InputStream in) {
    throw new TSFileDecodingException("Method readBoolean is not supproted by FloatDecoder");
  }

  @Override
  public short readShort(InputStream in) {
    throw new TSFileDecodingException("Method readShort is not supproted by FloatDecoder");
  }

  @Override
  public int readInt(InputStream in) {
    throw new TSFileDecodingException("Method readInt is not supproted by FloatDecoder");
  }

  @Override
  public long readLong(InputStream in) {
    throw new TSFileDecodingException("Method readLong is not supproted by FloatDecoder");
  }
}
