package cn.edu.tsinghua.tsfile.encoding.encoder;

import cn.edu.tsinghua.tsfile.common.exception.TSFileEncodingException;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Encoder for float or double value using rle or two diff
 * according to following grammar:
 * <pre>
 * {@code
 * float encoder: <maxPointvalue> <encoded-data>
 * maxPointvalue := number for accuracy of decimal places, store as unsigned var int
 * encoded-data := same as encoder's pattern
 * }
 * </pre>
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
    private double maxPointValue;

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
            } else if (dataType == TSDataType.DOUBLE) {
                encoder = new LongRleEncoder(EndianType.LITTLE_ENDIAN);
                LOGGER.debug("tsfile-encoding FloatEncoder: init encoder using long-rle and double");
            } else {
                throw new TSFileEncodingException(
                        String.format("data type %s is not supported by FloatEncoder", dataType));
            }
        } else if (encodingType == TSEncoding.TS_2DIFF) {
            if (dataType == TSDataType.FLOAT) {
                encoder = new DeltaBinaryEncoder.IntDeltaEncoder();
                LOGGER.debug("tsfile-encoding FloatEncoder: init encoder using int-delta and float");
            } else if (dataType == TSDataType.DOUBLE) {
                encoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                LOGGER.debug("tsfile-encoding FloatEncoder: init encoder using long-delta and double");
            } else {
                throw new TSFileEncodingException(
                        String.format("data type %s is not supported by FloatEncoder", dataType));
            }
        } else {
            throw new TSFileEncodingException(
                    String.format("%s encoding is not supported by FloatEncoder", encodingType));
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

    private void calculateMaxPonitNum() {
        if (maxPointNumber <= 0) {
            maxPointNumber = 0;
            maxPointValue = 1;
        } else {
            maxPointValue = Math.pow(10, maxPointNumber);
        }
    }

    private int convertFloatToInt(float value) {
        return (int) Math.round(value * maxPointValue);
    }

    private long convertDoubleToLong(double value) {
        return Math.round(value * maxPointValue);
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
    public int getOneItemMaxSize() {
        return encoder.getOneItemMaxSize();
    }

    @Override
    public long getMaxByteSize() {
        return encoder.getMaxByteSize();
    }

}
