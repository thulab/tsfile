package cn.edu.thu.tsfile.timeseries.write.desc;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.compress.Compressor;
import cn.edu.thu.tsfile.encoding.encoder.Encoder;
import cn.edu.thu.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.schema.converter.TSDataTypeConverter;
import cn.edu.thu.tsfile.timeseries.write.schema.converter.TSEncodingConverter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * This class describes a measurement's information registered in
 * {@linkplain FileSchema FilSchema}, including measurement id,
 * data type, encoding and compressor type. For each TSEncoding, MeasurementDescriptor maintains
 * respective TSEncodingConverter; For TSDataType, only ENUM has TSDataTypeConverter up to now.
 * 
 * @since version 0.1.0
 * @author kangrong
 *
 */
public class MeasurementDescriptor implements Comparable<MeasurementDescriptor> {
    private static final Logger LOG = LoggerFactory.getLogger(MeasurementDescriptor.class);
    private String measurementId;
    private final TSDataType type;
    private TSDataTypeConverter typeConverter;
    private final TSEncoding encoding;
    private TSEncodingConverter encodingConverter;
    private TSEncoding freqDomainEncoding;
    private TSEncodingConverter freqEncodingConverter;
    private Compressor compressor;
    private TSFileConfig conf;

    public MeasurementDescriptor(TSDataType type, String measurementId, TSEncoding encoding) {
        this.type = type;
        this.measurementId = measurementId;
        this.encoding = encoding;
        this.conf = TSFileDescriptor.getInstance().getConfig();
    }

    public MeasurementDescriptor(TSDataType type, String measurementId, TSEncoding encoding,
            JSONObject seriesObject) throws WriteProcessException {
        this(type, measurementId, encoding);
        // initialize TSDataType. e.g. set data values for enum type
        if (type == TSDataType.ENUMS) {
            typeConverter = TSDataTypeConverter.getConverter(type);
            typeConverter.initFromJsonObject(seriesObject);
        }
        // initialize TSEncoding. e.g. set max error for PLA and SDT
        encodingConverter = TSEncodingConverter.getConverter(encoding);
        encodingConverter.initFromJsonObject(measurementId, seriesObject);
        if (seriesObject.has(JsonFormatConstant.COMPRESS_TYPE)) {
            this.compressor =
                    Compressor.getCompressor(seriesObject
                            .getString(JsonFormatConstant.COMPRESS_TYPE));
        } else {
            this.compressor = Compressor.getCompressor(TSFileDescriptor.getInstance().getConfig().compressName);
        }
        // initialize frequency domain encoding
        if (seriesObject.has(JsonFormatConstant.FREQUENCY_ENCODING)) {
            this.freqDomainEncoding =
                    TSEncoding.valueOf(seriesObject
                            .getString(JsonFormatConstant.FREQUENCY_ENCODING));
            if(!TSEncodingConverter.freqEncodings.contains(freqDomainEncoding))
                throw new InvalidJsonSchemaException("invalid encoding for frequency domain:" + freqDomainEncoding);
            this.freqEncodingConverter = TSEncodingConverter.getConverter(this.freqDomainEncoding);
            seriesObject.put(JsonFormatConstant.DFT_WRITE_Main_FREQ, true);
            this.freqEncodingConverter.initFromJsonObject(measurementId, seriesObject);
        } else {
            this.freqDomainEncoding = null;
        }
    }

    public String getMeasurementId() {
        return measurementId;
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
     * return the max possible length of given type
     *
     * @return length in unit of byte
     */
    public int getTypeLength() {
        switch (type){
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
            case BYTE_ARRAY:
                // 4 is the length of string in type of Integer.
                // Note that one char corresponding to 3 byte is valid only in 16-bit BMP
                return conf.defaultMaxStringLength * TSFileConfig.byteSizePerChar + 4;
            case ENUMS:
                //every enum value is converted to integer
                return 4;
            case BIGDECIMAL:
                return 8;
            default:
                throw new UnSupportedDataTypeException(type.toString());
        }
    }

    public void setDataValues(VInTimeSeriesChunkMetaData v) {
        if (typeConverter != null)
            typeConverter.setDataValues(v);
    }

    public Encoder getTimeEncoder(){
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        TSEncoding timeSeriesEncoder = TSEncoding.valueOf(conf.timeSeriesEncoder);
        TSDataType timeType = TSDataType.valueOf(conf.defaultTimeType);
        Encoder timeEncoder =
                TSEncodingConverter.getConverter(timeSeriesEncoder)
                    .getEncoder(measurementId, timeType);
        return timeEncoder;
    }

    public Encoder getValueEncoder() {
        return encodingConverter.getEncoder(measurementId, type);
    }

    public Encoder getFreqEncoder() {
        if (this.freqDomainEncoding != null)
            return freqEncodingConverter.getEncoder(measurementId, type);
        else
            return null;
    }

    public Compressor getCompressor() {
        return compressor;
    }

    /**
     * Enum datum inputs a string value and returns its ordinal integer value.It's illegal that
     * other data type calling this method<br>
     * e.g. enum:[MAN(0),WOMAN(1)],calls parseEnumValue("WOMAN"),return 1
     * 
     * @param string - enum value in type of string
     * @return - ordinal integer in enum field
     */
    public int parseEnumValue(String string) {
        if (type != TSDataType.ENUMS) {
            LOG.error("type is not enums!return -1");
            return -1;
        }
        return ((TSDataTypeConverter.ENUMS) typeConverter).parseValue(string);
    }

    @Override
    public int hashCode() {
        return measurementId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof MeasurementDescriptor))
            return false;
        MeasurementDescriptor ot = (MeasurementDescriptor) other;
        return this.measurementId.equals(ot.measurementId);
    }

    @Override
    public int compareTo(MeasurementDescriptor o) {
        if (equals(o))
            return 0;
        else
            return this.measurementId.compareTo(o.measurementId);
    }

    @Override
    public String toString() {
        StringContainer sc = new StringContainer(",");
        sc.addTail("[", measurementId, type.toString(), encoding.toString(),
                encodingConverter.toString(), compressor.getCodecName().toString());
        if (freqDomainEncoding != null) {
            sc.addTail(freqDomainEncoding.toString());
            sc.addTail(freqEncodingConverter.toString());
        }
        if (typeConverter != null)
            sc.addTail(typeConverter.toString());
        sc.addTail("]");
        return sc.toString();
    }

}
