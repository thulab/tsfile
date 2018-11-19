package cn.edu.tsinghua.tsfile.timeseries.write.schema;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * FileSchema stores the schema of registered measurements and delta objects that appeared in this
 * stage. All delta objects written to the same TSFile have the same schema. FileSchema takes the
 * JSON schema file as a parameter and registers measurement information. FileSchema also records
 * all appeared delta object IDs in this stage.
 *
 * @author kangrong
 */
public class FileSchema {
  static private final Logger LOG = LoggerFactory.getLogger(FileSchema.class);

  /**
   * {@code Map<measurementId, MeasurementSchema>}
   */
  private Map<String, MeasurementSchema> measurementSchema;


  /**
   * init measurementSchema and additionalProperties as empty map
   */
  public FileSchema() {
    this.measurementSchema = new HashMap<>();
  }

  /**
   *   example:
   *   {
   *   "measurement_id": "sensor_cpu_50",
   *   "data_type": "INT32",
   *   "encoding": "RLE"
   *   }
   *
   *   {"schema":
   *    [
   *     {
   *      "measurement_id": "sensor_1",
   *      "data_type": "FLOAT",
   *      "encoding": "RLE"
   *     },
   *     {
   *       "measurement_id": "sensor_2",
   *       "data_type": "INT32",
   *       "encoding": "TS_2DIFF"
   *     },
   *     {
   *       "measurement_id": "sensor_3",
   *       "data_type": "INT32",
   *       "encoding": "TS_2DIFF"
   *     }
   *    ]
   *   };
   *
   * @param jsonSchema file schema in json format
   */
  @Deprecated
  public FileSchema(JSONObject jsonSchema) throws InvalidJsonSchemaException {
    this(JsonConverter.converterJsonToMeasurementDescriptors(jsonSchema));
  }

  /**
   * init additionalProperties and register measurements
   */
  public FileSchema(Map<String, MeasurementSchema> measurements) {
    this();
    this.registerMeasurements(measurements);
  }


  public TSDataType getMeasurementDataTypes(String measurementUID) {
    MeasurementSchema measurementSchema = this.measurementSchema.get(measurementUID);
    if(measurementSchema == null) {
      return null;
    }
    return measurementSchema.getType();

  }


  public Map<String, MeasurementSchema> getAllMeasurementSchema() {
    return measurementSchema;
  }


  /**
   * register a MeasurementSchema
   */
  public void registerMeasurement(MeasurementSchema descriptor) {
    // add to measurementSchema as <measurementID, MeasurementSchema>
    this.measurementSchema.put(descriptor.getMeasurementId(), descriptor);
  }

  /**
   * register all MeasurementSchema in input map
   */
  private void registerMeasurements(Map<String, MeasurementSchema> measurements) {
    measurements.forEach((id, md) -> registerMeasurement(md));
  }

  /**
   * check is this schema contains input measurementID
   */
  public boolean hasMeasurement(String measurementId) {
    return measurementSchema.containsKey(measurementId);
  }

}
