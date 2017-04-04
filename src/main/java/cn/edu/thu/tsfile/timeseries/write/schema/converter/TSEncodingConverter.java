package cn.edu.thu.tsfile.timeseries.write.schema.converter;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.exception.metadata.MetadataArgsErrorException;
import cn.edu.thu.tsfile.encoding.common.EndianType;
import cn.edu.thu.tsfile.encoding.encoder.BitmapEncoder;
import cn.edu.thu.tsfile.encoding.encoder.DeltaBinaryEncoder;
import cn.edu.thu.tsfile.encoding.encoder.Encoder;
import cn.edu.thu.tsfile.encoding.encoder.FloatEncoder;
import cn.edu.thu.tsfile.encoding.encoder.IntRleEncoder;
import cn.edu.thu.tsfile.encoding.encoder.LongRleEncoder;
import cn.edu.thu.tsfile.encoding.encoder.PlainEncoder;
import cn.edu.thu.tsfile.encoding.encoder.dft.DFTDoubleEncoder;
import cn.edu.thu.tsfile.encoding.encoder.dft.DFTEncoder;
import cn.edu.thu.tsfile.encoding.encoder.dft.DFTFloatEncoder;
import cn.edu.thu.tsfile.encoding.encoder.dft.DFTIntEncoder;
import cn.edu.thu.tsfile.encoding.encoder.dft.DFTLongEncoder;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;

import java.util.HashSet;
import java.util.Set;

import static cn.edu.thu.tsfile.common.constant.JsonFormatConstant.DFT_PACK_LENGTH;
import static cn.edu.thu.tsfile.common.constant.JsonFormatConstant.DFT_RATE;
import static cn.edu.thu.tsfile.common.constant.JsonFormatConstant.DFT_WRITE_Main_FREQ;
import static cn.edu.thu.tsfile.common.constant.JsonFormatConstant.DFT_WRITE_ENCODING;
import static cn.edu.thu.tsfile.common.constant.JsonFormatConstant.DFT_OVERLAP_RATE;
import static cn.edu.thu.tsfile.common.constant.JsonFormatConstant.DFT_MAIN_FREQ_NUM;

/**
 * Each subclass of TSEncodingConverter responds a enumerate value in
 * {@linkplain TSEncoding TSEncoding}, which stores several
 * configuration related to responding encoding type to generate
 * {@linkplain Encoder Encoder} instance.<br>
 * Each TSEncoding has a responding TSEncodingConverter. The design referring to visit pattern
 * provides same outer interface for different TSEncodings and gets rid of the duplicate switch-case
 * code.
 * 
 * @author kangrong
 *
 */
public abstract class TSEncodingConverter {
    public static Set<TSEncoding> freqEncodings = new HashSet<TSEncoding>(){{
        add(TSEncoding.DFT);
    }};

    public static class PLAIN extends TSEncodingConverter {
        private int maxStringLength;
        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            return new PlainEncoder(EndianType.LITTLE_ENDIAN, type, maxStringLength);
        }

        @Override
        public void initFromJsonObject(String measurementId, JSONObject seriesObject) {
            // set max error from initialized map or default value if not set
            if (!seriesObject.has(JsonFormatConstant.MAX_STRING_LENGTH)) {
                maxStringLength = conf.defaultMaxStringLength;
            } else {
                maxStringLength = seriesObject.getInt(JsonFormatConstant.MAX_STRING_LENGTH);
                if (maxStringLength < 0) {
                    maxStringLength = conf.defaultMaxStringLength;
                    LOG.warn(
                            "cannot set max string length to negative value, replaced with default value:{}",
                            maxStringLength);
                }
            }
        }
    }

    public static class RLE extends TSEncodingConverter {
        private int maxPointNumber = 0;

        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            switch (type) {
                case INT32:
                    return new IntRleEncoder(EndianType.LITTLE_ENDIAN);
                case INT64:
                    return new LongRleEncoder(EndianType.LITTLE_ENDIAN);
                case FLOAT:
                    return new FloatEncoder(TSEncoding.RLE, TSDataType.FLOAT, maxPointNumber);
                case DOUBLE:
                case BIGDECIMAL:
                    return new FloatEncoder(TSEncoding.RLE, type, maxPointNumber);
                default:
                    throw new UnSupportedDataTypeException("RLE doesn't support data type: "+ type);
            }
        }

        /**
         * RLE could specify <b>max_point_number</b> in given JSON Object, which means the maximum
         * decimal digits for float or double data.
         */
        @Override
        public void initFromJsonObject(String measurementId, JSONObject seriesObject) {
            // set max error from initialized map or default value if not set
            if (!seriesObject.has(JsonFormatConstant.MAX_POINT_NUMBER)) {
                maxPointNumber = conf.defaultMaxPointNumber;
            } else {
                maxPointNumber = seriesObject.getInt(JsonFormatConstant.MAX_POINT_NUMBER);
                if (maxPointNumber < 0) {
                    maxPointNumber = conf.defaultMaxPointNumber;
                    LOG.warn(
                            "cannot set max point number to negative value, replaced with default value:{}",
                            maxPointNumber);
                }
            }
        }

        @Override
        /**
         * RLE could specify <b>max_point_number</b> as parameter, which means the maximum
         * decimal digits for float or double data.
         */
        public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
            if (JsonFormatConstant.MAX_POINT_NUMBER.equals(pmKey)) {
                try {
                    return Integer.valueOf(value);
                } catch (NumberFormatException e) {
                    throw new MetadataArgsErrorException("paramter " + pmKey
                            + " meets error integer format :" + value);
                }
            } else
                throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
        }

        @Override
        public String toString() {
            return "maxPointNumber:" + maxPointNumber;
        }
    }

    public static class TS_2DIFF extends TSEncodingConverter {
        private int maxPointNumber = 0;

        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            switch (type) {
                case INT32:
                    return new DeltaBinaryEncoder.IntDeltaEncoder();
                case INT64:
                    return new DeltaBinaryEncoder.LongDeltaEncoder();
                case FLOAT:
                case DOUBLE:
                case BIGDECIMAL:
                    return new FloatEncoder(TSEncoding.TS_2DIFF, type, maxPointNumber);
                default:
                    throw new UnSupportedDataTypeException("TS_2DIFF doesn't support data type: "+ type);
            }
        }

        @Override
        /**
         * TS_2DIFF could specify <b>max_point_number</b> in given JSON Object, which means the maximum
         * decimal digits for float or double data.
         */
        public void initFromJsonObject(String measurementId, JSONObject seriesObject) {
            // set max error from initialized map or default value if not set
            TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
            if (!seriesObject.has(JsonFormatConstant.MAX_POINT_NUMBER)) {
                maxPointNumber = conf.defaultMaxPointNumber;
            } else {
                maxPointNumber = seriesObject.getInt(JsonFormatConstant.MAX_POINT_NUMBER);
                if (maxPointNumber < 0) {
                    maxPointNumber = conf.defaultMaxPointNumber;
                    LOG.warn(
                            "cannot set max point number to negative value, replaced with default value:{}",
                            maxPointNumber);
                }
            }
        }

        @Override
        /**
         * TS_2DIFF could specify <b>max_point_number</b> as parameter, which means the maximum
         * decimal digits for float or double data.
         */
        public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
            if (JsonFormatConstant.MAX_POINT_NUMBER.equals(pmKey)) {
                try {
                    return Integer.valueOf(value);
                } catch (NumberFormatException e) {
                    throw new MetadataArgsErrorException("paramter " + pmKey
                            + " meets error integer format :" + value);
                }
            } else
                throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
        }

        @Override
        public String toString() {
            return "maxPointNumber:" + maxPointNumber;
        }

    }

    public static class BITMAP extends TSEncodingConverter {
        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            switch (type){
                case ENUMS:
                    return new BitmapEncoder(EndianType.LITTLE_ENDIAN);
                default:
                    throw new UnSupportedDataTypeException("BITMAP doesn't support data type: "+ type);
            }
        }
    }
    public static class DFT extends TSEncodingConverter {
        private int packLength = -1;
        private double rate = 0;
        private boolean isWriteMainFreq = false;
        private boolean isEncoding = true;
        private float overlapRate = 0;
        private int mainFreqNum = 1;

        @Override
        public void initFromJsonObject(String measurementId, JSONObject seriesObject) {
            // set pack length from initialized map or default value if not set
            if (!seriesObject.has(DFT_PACK_LENGTH)) {
                packLength = conf.defaultDFTPackLength;
                LOG.warn("DFT doesn't specify pack length,set default value:{}", packLength);
            } else {
                packLength = seriesObject.getInt(DFT_PACK_LENGTH);
                if (packLength < 0) {
                    packLength = conf.defaultDFTPackLength;
                    LOG.warn(
                            "cannot set DFT pack length to negative value, replaced with default value:{}",
                            packLength);
                }
            }
            if (!seriesObject.has(DFT_RATE)) {
                rate = conf.defaultDFTRate;
                LOG.warn("DFT doesn't specify rate,set default value:{}", rate);
            } else {
                rate = seriesObject.getDouble(DFT_RATE);
                if (rate <= 0 || rate > 1) {
                    rate = conf.defaultDFTRate;
                    LOG.warn("cannot set DFT rate outside [0,1], replaced with default value:{}",
                            rate);
                }
            }
            if (!seriesObject.has(JsonFormatConstant.DFT_OVERLAP_RATE)) {
                overlapRate = conf.defaultDFTOverlapRate;
                LOG.warn("DFT doesn't specify overlap rate, set default value:{}", overlapRate);
            } else {
                overlapRate = (float) seriesObject.getDouble(JsonFormatConstant.DFT_OVERLAP_RATE);
            }

            if (!seriesObject.has(DFT_WRITE_Main_FREQ)) {
                isWriteMainFreq = conf.defaultDFTWriteMain;
                LOG.warn("DFT doesn't specify boolean main,set default value:{}", isWriteMainFreq);
            } else {
                isWriteMainFreq = seriesObject.getBoolean(DFT_WRITE_Main_FREQ);
            }

            if (!seriesObject.has(JsonFormatConstant.DFT_WRITE_ENCODING)) {
                isEncoding = conf.defaultDFTWriteEncoding;
                LOG.warn("DFT doesn't specify whether writing encoding, set default value:{}",
                        isEncoding);
            } else {
                isEncoding = seriesObject.getBoolean(JsonFormatConstant.DFT_WRITE_ENCODING);
            }

            if (!seriesObject.has(JsonFormatConstant.DFT_MAIN_FREQ_NUM)) {
                mainFreqNum = conf.defaultDFTMainFreqNum;
                LOG.warn("DFT doesn't specify main frequency number, set default value:{}",
                        mainFreqNum);
            } else {
                mainFreqNum = seriesObject.getInt(JsonFormatConstant.DFT_MAIN_FREQ_NUM);
                if (mainFreqNum < 0) {
                    mainFreqNum = conf.defaultDFTMainFreqNum;
                    LOG.warn(
                            "cannot set DFT main frequency number to negative value, replaced with default value:{}",
                            mainFreqNum);
                }
            }
        }

        @Override
        public Encoder getEncoder(String measurementId, TSDataType type) {
            // get max error from initialized map or default value if not set
            if (packLength == -1) {
                packLength = conf.defaultDFTPackLength;
                LOG.warn("DFT doesn't initialize pack length,use default value:{}", packLength);
            }
            DFTEncoder<?> dft;
            switch (type) {
                case INT32:
                    dft = new DFTIntEncoder(packLength, rate, overlapRate);
                    break;
                case INT64:
                    dft = new DFTLongEncoder(packLength, rate, overlapRate);
                    break;
                case FLOAT:
                    dft = new DFTFloatEncoder(packLength, rate, overlapRate);
                    break;
                case DOUBLE:
                    dft = new DFTDoubleEncoder(packLength, rate, overlapRate);
                    break;
                default:
                    throw new UnSupportedDataTypeException("DFT doesn't support data type: "+ type);
            }
            dft.setIsEncoding(isEncoding);
            dft.setIsWriteMainFreq(isWriteMainFreq);
            dft.setMainFreqNum(mainFreqNum);
            return dft;
        }

        @Override
        public Object checkParameter(String pmKey, String value)
                throws MetadataArgsErrorException {
            try {
                switch (pmKey) {
                    case DFT_PACK_LENGTH:
                        return Integer.valueOf(value);
                    case DFT_RATE:
                        return Double.valueOf(value);
                    case DFT_WRITE_Main_FREQ:
                        return Boolean.valueOf(value);
                    case DFT_WRITE_ENCODING:
                        return Boolean.valueOf(value);
                    case DFT_OVERLAP_RATE:
                        return Float.valueOf(value);
                    case DFT_MAIN_FREQ_NUM:
                        return Integer.valueOf(value);
                    default:
                        throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
                }
            } catch (Exception e) {
                throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
            }
        }

        @Override
        public String toString() {
            StringContainer sc = new StringContainer(",");
            sc.addTail("DFT_PACK_LENGTH", packLength);
            sc.addTail("DFT_RATE", rate);
            sc.addTail("DFT_WRITE_Main_FREQ", isWriteMainFreq);
            sc.addTail("DFT_WRITE_ENCODING", isEncoding);
            sc.addTail("DFT_OVERLAP_RATE", overlapRate);
            sc.addTail("DFT_MAIN_FREQ_NUM", mainFreqNum);
            return sc.toString();
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(TSEncodingConverter.class);
    protected final TSFileConfig conf;

    public TSEncodingConverter() {
        this.conf = TSFileDescriptor.getInstance().getConfig();
    }

    /**
     * return responding TSEncodingConverter from a TSEncoding
     * 
     * @param type - given encoding type
     * @return - responding TSEncodingConverter
     */
    public static TSEncodingConverter getConverter(TSEncoding type) {
        switch (type) {
            case PLAIN:
                return new PLAIN();
            case RLE:
                return new RLE();
            case TS_2DIFF:
                return new TS_2DIFF();
            case BITMAP:
                return new BITMAP();
            // TODO to be add after adding PLA,SDT
            // case PLA:
            // return new PLA();
            // case SDT:
            // return new SDT();
            case DFT:
                return new DFT();
            default:
                throw new UnsupportedOperationException(type.toString());
        }
    }

    /**
     * return a series's encoder with different types and parameters according to its measurement id
     * and data type
     * 
     * @param measurementId - given measurement id
     * @param type - given data type
     * @return - return a {@linkplain Encoder Encoder}
     */
    public abstract Encoder getEncoder(String measurementId, TSDataType type);

    /**
     * for TSEncoding, JSON is a kind of type for initialization. {@code InitFromJsonObject} gets
     * values from JSON object which will be used latter.<br>
     * if this type has extra parameters to construct, override it.
     * 
     * @param measurementId - measurement id to be added.
     * @param seriesObject - JSON object in FileSchema's file
     */
    public void initFromJsonObject(String measurementId, JSONObject seriesObject) {}

    /**
     * For a TSEncodingConverter, check the input parameter. If it's valid, return this parameter in
     * its appropriate type. This method needs to be extended.
     * 
     * @param pmKey - argument key in JSON object key-value pair
     * @param value - argument value in JSON object key-value pair in type of String
     * @return - default return is null which means this data type needn't the parameter
     */
    public Object checkParameter(String pmKey, String value) throws MetadataArgsErrorException {
        throw new MetadataArgsErrorException("don't need args:{}" + pmKey);
    }

    /**
     * check the validity of input parameter. If it's valid, return this parameter in its
     * appropriate type.
     * 
     * @param encoding - encoding type
     * @param pmKey - argument key in JSON object key-value pair
     * @param value - argument value in JSON object key-value pair in type of String
     * @return - argument value in JSON object key-value pair in its suitable type
     */
    public static Object checkParameter(TSEncoding encoding, String pmKey, String value)
            throws MetadataArgsErrorException {
        return getConverter(encoding).checkParameter(pmKey, value);
    }

    @Override
    public String toString() {
        return "";
    }
}
