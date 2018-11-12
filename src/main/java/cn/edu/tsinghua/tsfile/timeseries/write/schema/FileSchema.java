package cn.edu.tsinghua.tsfile.timeseries.write.schema;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.InvalidJsonSchemaException;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.JsonConverter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
   * {@code Map<measurementId, MeasurementDescriptor>}
   */
  private Map<String, MeasurementDescriptor> measurementNameDescriptorMap;

  /** all metadatas of TimeSeries **/
  private List<TimeSeriesMetadata> tsMetadata = new ArrayList<>();

  /**
   * init measurementNameDescriptorMap and additionalProperties as empty map
   */
  public FileSchema() {
    this.measurementNameDescriptorMap = new HashMap<>();
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
  public FileSchema(Map<String, MeasurementDescriptor> measurements) {
    this();
    this.registerMeasurements(measurements);
  }


  public TSDataType getMeasurementDataTypes(String measurementUID) {
    MeasurementDescriptor measurementDescriptor = measurementNameDescriptorMap.get(measurementUID);
    if(measurementDescriptor == null) {
      return null;
    }
    return measurementDescriptor.getType();

  }

  public MeasurementDescriptor getMeasurementDescriptor(String measurementUID) {
    return measurementNameDescriptorMap.get(measurementUID);
  }

  public Map<String, MeasurementDescriptor> getDescriptor() {
    return measurementNameDescriptorMap;
  }

  /**
   * add a TimeSeriesMetadata into this fileSchema
   *
   * @param measurementId the measurement id of this TimeSeriesMetadata
   * @param type the data type of this TimeSeriesMetadata
   */
  private void addTimeSeriesMetadata(String measurementId, TSDataType type) {
    TimeSeriesMetadata ts = new TimeSeriesMetadata(measurementId, type);
    LOG.debug("add Time Series:{}", ts);
    this.tsMetadata.add(ts);
  }

  public List<TimeSeriesMetadata> getTimeSeriesMetadatas() {
    return tsMetadata;
  }

  /**
   * register a MeasurementDescriptor
   */
  public void registerMeasurement(MeasurementDescriptor descriptor) {
    // add to measurementNameDescriptorMap as <measurementID, MeasurementDescriptor>
    this.measurementNameDescriptorMap.put(descriptor.getMeasurementId(), descriptor);

    // add time series metadata
    this.addTimeSeriesMetadata(descriptor.getMeasurementId(), descriptor.getType());
  }

  /**
   * register all MeasurementDescriptor in input map
   */
  private void registerMeasurements(Map<String, MeasurementDescriptor> measurements) {
    measurements.forEach((id, md) -> registerMeasurement(md));
  }

  /**
   * check is this schema contains input measurementID
   */
  public boolean hasMeasurement(String measurementId) {
    return measurementNameDescriptorMap.containsKey(measurementId);
  }

}
