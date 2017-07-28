package cn.edu.thu.tsfile.timeseries.write.schema.converter;

import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * JsonConverter is used to convert JsonObject to TSFile Schema what a java
 * class defined in tsfile project. the main function of this converter is to
 * receive a json object of schema and register all measurements.
 * <p>
 * The format of JSON schema is as follow:
 * </p>
 * <p>
 * <pre>
 *  {
 *     "schema": [
 *        {
 *          "measurement_id": "s1",
 *          "data_type": "INT32",
 *          "encoding": "RLE"
 *         },
 *         ...
 *     ],
 *     "delta_type": "type"
 * }
 *
 * </pre>
 *
 * @author kangrong
 * @see FileSchema FileSchema
 * @see TSDataTypeConverter TSDataTypeConverter
 * @see TSEncodingConverter TSEncodingConverter
 */
public class JsonConverter {

    private static final Logger LOG = LoggerFactory.getLogger(JsonConverter.class);

    /**
     * input a FileSchema and a jsonObject to be converted, register the
     * measurement information.
     *
     * @param jsonSchema the whole schema in type of JSONObject
     * @param fileSchema the FileSchema to be set
     */
    public static void converterJsonToSchema(JSONObject jsonSchema, FileSchema fileSchema)
            throws InvalidJsonSchemaException {
        if (!jsonSchema.has(JsonFormatConstant.JSON_SCHEMA))
            throw new InvalidJsonSchemaException("missing fields:" + JsonFormatConstant.JSON_SCHEMA);
        //set deltaType
        fileSchema.setDeltaType(jsonSchema.has(JsonFormatConstant.DELTA_TYPE)
                ? jsonSchema.getString(JsonFormatConstant.DELTA_TYPE) : JsonFormatConstant.defaultDeltaType);
        //get series and set currentRowMaxSize
        int currentRowMaxSize = 0;
        JSONArray schemaArray = jsonSchema.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
        for (int i = 0; i < schemaArray.length(); i++) {
            currentRowMaxSize += registerMeasurement(fileSchema, schemaArray.getJSONObject(i));
        }
        LOG.debug("set one row max size of a file schema to be:{}", currentRowMaxSize);
        fileSchema.setCurrentRowMaxSize(currentRowMaxSize);
        //add properties
        if (jsonSchema.has(JsonFormatConstant.PROPERTIES)) {
            JSONObject jsonProps = jsonSchema.getJSONObject(JsonFormatConstant.PROPERTIES);
            for (Object key : jsonProps.keySet())
                fileSchema.addProp(key.toString(), jsonProps.get(key.toString()).toString());
        }
    }

    /**
     * * for each measurement, it may have some information to be initialized.
     * <br>
     * e.g.<br>
     * while parsing a string to TSRecord, {@linkplain TSRecord RecordParser}
     * should know its value type like integer or float by measurement id. If
     * the type is enumerate, the parser needs get enumerate values like
     * Color:{RED, GREEN, BLUE}<br>
     * another example is: <br>
     * each measurement should specify its encoder and compressing method. If
     * its encoder or compressor needs parameters, these extra parameters should
     * also be registered.
     *
     * @param fileSchema     - FileSchema to be set
     * @param measurementObj - JSON object of this measurement id in given schema
     * @return the max size of this measurement
     */
    private static int registerMeasurement(FileSchema fileSchema, JSONObject measurementObj) {
        if (!measurementObj.has(JsonFormatConstant.MEASUREMENT_UID) && !measurementObj.has(JsonFormatConstant.DATA_TYPE)
                && !measurementObj.has(JsonFormatConstant.MEASUREMENT_ENCODING)) {
            LOG.warn("The format of given json is error. Give up to register this measurement. Given json:{}",
                    measurementObj);
            return 0;
        }
        // register series info to fileSchema
        String measurementId = measurementObj.getString(JsonFormatConstant.MEASUREMENT_UID);
        TSDataType type = TSDataType.valueOf(measurementObj.getString(JsonFormatConstant.DATA_TYPE));
        fileSchema.addSeries(measurementId, type);
        // add TimeSeries into metadata
        fileSchema.addTimeSeriesMetadata(measurementId, type);
        // encoding information
        TSEncoding encoding = TSEncoding.valueOf(measurementObj.getString(JsonFormatConstant.MEASUREMENT_ENCODING));
        // all information of one series
        Map<String, String> props = new HashMap<>();
        for (Object key : measurementObj.keySet()) {
            String value = measurementObj.get(key.toString()).toString();
            props.put(key.toString(), value);
        }
        MeasurementDescriptor md = new MeasurementDescriptor(measurementId, type, encoding, props);
        fileSchema.setDescriptor(measurementId, md);
        return md.getTimeEncoder().getOneItemMaxSize() + md.getValueEncoder().getOneItemMaxSize();
    }
}
