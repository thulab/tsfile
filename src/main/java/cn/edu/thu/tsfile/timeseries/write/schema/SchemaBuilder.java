package cn.edu.thu.tsfile.timeseries.write.schema;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;

import java.util.Map;

/**
 * Used to build FileSchema of tsfile
 *
 * @author qiaojialin
 */
public class SchemaBuilder {
    private FileSchema fileSchema;

    public SchemaBuilder() {
        fileSchema = new FileSchema();
    }

    /**
     * get file schema after adding all series and properties
     *
     * @return constructed file schema
     */
    public FileSchema getFileSchema() {
        return fileSchema;
    }

    /**
     * add one series to tsfile schema
     *
     * @param measurementId (not null) id of the series
     * @param dataType (not null) series data type
     * @param tsEncoding (not null) encoding method you specified
     * @param props information in encoding method
     */
    public void addSeries(String measurementId, TSDataType dataType, TSEncoding tsEncoding, Map<String, String> props) {
        fileSchema.addSeries(measurementId, dataType);
        fileSchema.addTimeSeriesMetadata(measurementId, dataType);
        MeasurementDescriptor md = new MeasurementDescriptor(dataType, measurementId, tsEncoding, props);
        fileSchema.setDescriptor(measurementId, md);
        int maxSize = md.getTimeEncoder().getOneItemMaxSize() + md.getValueEncoder().getOneItemMaxSize();
        fileSchema.addCurrentRowMaxSize(maxSize);
    }

    /**
     * add one series to tsfile schema
     *
     * @param measurementId (not null) id of the series
     * @param dataType (not null) series data type
     * @param encoding (not null) encoding method you specified
     * @param props information in encoding method
     */
    public void addSeries(String measurementId, TSDataType dataType, String encoding, Map<String, String> props) {
        fileSchema.addSeries(measurementId, dataType);
        fileSchema.addTimeSeriesMetadata(measurementId, dataType);
        TSEncoding tsEncoding = TSEncoding.valueOf(encoding);
        MeasurementDescriptor md = new MeasurementDescriptor(dataType, measurementId, tsEncoding, props);
        fileSchema.setDescriptor(measurementId, md);
        int maxSize = md.getTimeEncoder().getOneItemMaxSize() + md.getValueEncoder().getOneItemMaxSize();
        fileSchema.addCurrentRowMaxSize(maxSize);
    }

    public void addProp(String key, String value) {
        fileSchema.addProp(key, value);
    }

    public void setProps(Map<String, String> props) {
        fileSchema.setProps(props);
    }

}
