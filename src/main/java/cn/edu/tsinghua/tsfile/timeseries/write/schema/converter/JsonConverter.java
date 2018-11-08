package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;

/**
 * <p>
 * JsonConverter is used to convert JsonObject to TSFile Schema which is a java class defined in tsfile
 * project. the main function of this converter is to receive a json object of schema and register
 * all measurements.
 * </p>
 *
 * The format of JSON schema is as follow:
 *
 * <pre>
 *  {
 *     "schema": [
 *        {
 *          "measurement_id": "s1",
 *          "data_type": "INT32",
 *          "encoding": "RLE"
 *         },
 *         {
 *             "measurement_id": "s3",
 *             "data_type": "ENUMS",
 *             "encoding": "BITMAP",
 *             "compressor": "SNAPPY",
 *             "enum_values":["MAN","WOMAN"],
 *             "max_error":12,
 *             "max_point_number":3
 *         },
 *         ...
 *     ],
 *     "properties":{
 *         "k1":"v1",
 *         "k2":"v2"
 *     }
 * }
 *
 * </pre>
 *
 * @author kangrong
 * @see TSDataTypeConverter TSDataTypeConverter
 * @see TSEncodingConverter TSEncodingConverter
 */
public class JsonConverter {

  private static final Logger LOG = LoggerFactory.getLogger(JsonConverter.class);

  /**
   * convert the input jsonObject to Map<measurementID, MeasurementDescriptor>
   *
   * @param jsonSchema
   *          the whole schema in type of JSONObject
   * @throws InvalidJsonSchemaException
   *           throw exception when json schema is not valid
   * @return converted measurement descriptors
   */
  public static Map<String, MeasurementDescriptor> converterJsonToMeasurementDescriptors(
      JSONObject jsonSchema) throws InvalidJsonSchemaException {
    Map<String, MeasurementDescriptor> result = new HashMap<>();
    // the input JSONObject must have JSON_SCHEMA
    if (!jsonSchema.has(JsonFormatConstant.JSON_SCHEMA))
      throw new InvalidJsonSchemaException("missing fields:" + JsonFormatConstant.JSON_SCHEMA);

    /**
     * get schema of all measurements in JSONArray from JSONObject
     *
     * "schema": [
     *         {
     *             "measurement_id": "s1",
     *             "data_type": "INT32",
     *             "encoding": "RLE"
     *         },
     *         {
     *             "measurement_id": "s2",
     *             "data_type": "INT64",
     *             "encoding": "TS_2DIFF"
     *         }...
     *  ]
     */
    JSONArray schemaArray = jsonSchema.getJSONArray(JsonFormatConstant.JSON_SCHEMA);
    // loop all JSONObject in this JSONArray and convert it to MeasurementDescriptor
    for (int i = 0; i < schemaArray.length(); i++) {
      MeasurementDescriptor mDescriptor = convertJsonToMeasureMentDescriptor(
          schemaArray.getJSONObject(i));
      result.put(mDescriptor.getMeasurementId(), mDescriptor);
    }
    return result;
  }

  /**
   * convert the input JSONObject to FileSchema Properties
   * @param jsonSchema the whole schema in form of JSONObject
   * @return converted properties
   */
  public static Map<String, String> convertJsonToSchemaProperties(JSONObject jsonSchema) {
    Map<String, String> result = new HashMap<>();
    // if input jsonSchema doesn't have PROPERTIES, return an empty Map
    if (jsonSchema.has(JsonFormatConstant.PROPERTIES)) {

      /**
       * get properties in JSONObject form from jsonSchema
       *
       * "properties":{
       *         "k1":"v1",
       *         "k2":"v2"
       *     }
       */
      JSONObject jsonProps = jsonSchema.getJSONObject(JsonFormatConstant.PROPERTIES);
      // loop all kv pairs in this jsonProps and put it into result Map
      for (Object key : jsonProps.keySet())
        result.put(key.toString(), jsonProps.get(key.toString()).toString());
    }
    return result;
  }

  /**
   * convert the input JSONObject to MeasurementDescriptor
   * @param measurementObj properties of one measurement
   *
   *  an example:
   *
   *  {
   *             "measurement_id": "s3",
   *             "data_type": "ENUMS",
   *             "encoding": "BITMAP",
   *
   *             // some measurement may have some properties
   *
   *             "compressor": "SNAPPY",
   *             "enum_values":["MAN","WOMAN"],
   *             "max_error":12,
   *             "max_point_number":3
   *  }
   *
   * @return converted MeasurementDescriptor
   */
  public static MeasurementDescriptor convertJsonToMeasureMentDescriptor(
      JSONObject measurementObj) {
    // input measurementObj must have MEASUREMENT_UID or DATA_TYPE or MEASUREMENT_ENCODING
    if (!measurementObj.has(JsonFormatConstant.MEASUREMENT_UID)
        && !measurementObj.has(JsonFormatConstant.DATA_TYPE)
        && !measurementObj.has(JsonFormatConstant.MEASUREMENT_ENCODING)) {
      LOG.warn(
          "The format of given json is error. Give up to register this measurement. Given json:{}",
          measurementObj);
      return null;
    }
    // get measurementID
    String measurementId = measurementObj.getString(JsonFormatConstant.MEASUREMENT_UID);
    // get data type information
    TSDataType type = TSDataType.valueOf(measurementObj.getString(JsonFormatConstant.DATA_TYPE));
    // get encoding information
    TSEncoding encoding = TSEncoding
        .valueOf(measurementObj.getString(JsonFormatConstant.MEASUREMENT_ENCODING));
    // put all kv pairs of this measurementObj into props Map
    Map<String, String> props = new HashMap<>();
    for (Object key : measurementObj.keySet()) {
      String value = measurementObj.get(key.toString()).toString();
      props.put(key.toString(), value);
    }
    // create a new MeasurementDescriptor and return it
    return new MeasurementDescriptor(measurementId, type, encoding, props);
  }

  /**
   * given a FileSchema and convert it to a JSONObject
   *
   * @param fileSchema
   *          the given schema in type of {@linkplain FileSchema FileSchema}
   * @return converted FileSchema in form of JSONObject
   */
  public static JSONObject converterFileSchemaToJson(
          FileSchema fileSchema) {
    /** JSONObject form of FileSchema **/
    JSONObject ret = new JSONObject();
    /** JSONObject form of all MeasurementDescriptors in fileSchema **/
    JSONArray jsonSchema = new JSONArray();
    /** JSONObject form of all properties in fileSchema **/
    JSONObject jsonProperties = new JSONObject();

    // convert all MeasurementDescriptors
    for (MeasurementDescriptor measurementDescriptor : fileSchema.getDescriptor().values()) {
      jsonSchema.put(convertMeasurementDescriptorToJson(measurementDescriptor));
    }
    // convert all properties
    fileSchema.getProps().forEach(jsonProperties::put);

    // put all MeasurementDescriptors and properties in result JSONObject
    ret.put(JsonFormatConstant.JSON_SCHEMA, jsonSchema);
    ret.put(JsonFormatConstant.PROPERTIES, jsonProperties);
    return ret;
  }

  /**
   * given a MeasurementDescriptor and convert it to a JSONObject
   * @param measurementDescriptor the given descriptor in type of {@linkplain MeasurementDescriptor MeasurementDescriptor}
   * @return converted MeasurementDescriptor in form of JSONObject
   *
   *  an example:
   *
   *  {
   *             "measurement_id": "s3",
   *             "data_type": "ENUMS",
   *             "encoding": "BITMAP",
   *
   *             // some measurement may have some properties
   *
   *             "compressor": "SNAPPY",
   *             "enum_values":["MAN","WOMAN"],
   *             "max_error":12,
   *             "max_point_number":3
   *  }
   */
  private static JSONObject convertMeasurementDescriptorToJson(
          MeasurementDescriptor measurementDescriptor) {
    JSONObject measurementObj = new JSONObject();
    // put measurementID, data type, encoding info and properties into result JSONObject
    measurementObj.put(JsonFormatConstant.MEASUREMENT_UID, measurementDescriptor.getMeasurementId());
    measurementObj.put(JsonFormatConstant.DATA_TYPE, measurementDescriptor.getType().toString());
    measurementObj.put(JsonFormatConstant.MEASUREMENT_ENCODING, measurementDescriptor.getEncodingType().toString());
    measurementDescriptor.getProps().forEach(measurementObj::put);
    return measurementObj;
  }
}
