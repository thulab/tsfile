package cn.edu.thu.tsfile.timeseries.write.schema;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;

import java.util.Map;

/**
 * This class is used to build FileSchema of tsfile
 *
 * @author qiaojialin
 */
public class SchemaBuilder {
    private FileSchema fileSchema;

    public SchemaBuilder() {
        fileSchema = new FileSchema();
        fileSchema.setDeltaType(SystemConstant.DEFAULT_DELTA_TYPE);
    }

    /**
     * add one series to tsfile schema
     *
     * @param measurementId (not null) id of the series
     * @param dataType      (not null) series data type
     * @param tsEncoding    (not null) encoding method you specified
     * @param props         information in encoding method
     */
    public SchemaBuilder addSeries(String measurementId, TSDataType dataType, TSEncoding tsEncoding,
                                   Map<String, String> props) {
        fileSchema.addSeries(measurementId, dataType);
        fileSchema.addTimeSeriesMetadata(measurementId, dataType);
        MeasurementDescriptor md = new MeasurementDescriptor(measurementId, dataType, tsEncoding, props);
        fileSchema.setDescriptor(measurementId, md);
        int maxSize = md.getTimeEncoder().getOneItemMaxSize() + md.getValueEncoder().getOneItemMaxSize();
        fileSchema.addCurrentRowMaxSize(maxSize);
        return this;
    }


    /**
     * MeasurementDescriptor is the schema of one series
     *
     * @param descriptor series schema
     * @return schema builder
     */
    public SchemaBuilder addSeries(MeasurementDescriptor descriptor) {
        fileSchema.addSeries(descriptor.getMeasurementId(), descriptor.getType());
        fileSchema.addTimeSeriesMetadata(descriptor.getMeasurementId(), descriptor.getType());
        fileSchema.setDescriptor(descriptor.getMeasurementId(), descriptor);
        int maxSize = descriptor.getTimeEncoder().getOneItemMaxSize() + descriptor.getValueEncoder().getOneItemMaxSize();
        fileSchema.addCurrentRowMaxSize(maxSize);
        return this;
    }


    /**
     * add one series to tsfile schema
     *
     * @param measurementId (not null) id of the series
     * @param dataType      (not null) series data type
     * @param encoding      (not null) encoding method you specified
     * @param props         information in encoding method
     */
    public SchemaBuilder addSeries(String measurementId, TSDataType dataType, String encoding,
                                   Map<String, String> props) {
        TSEncoding tsEncoding = TSEncoding.valueOf(encoding);
        addSeries(measurementId, dataType, tsEncoding, props);
        return this;
    }

    public SchemaBuilder addProp(String key, String value) {
        fileSchema.addProp(key, value);
        return this;
    }

    public SchemaBuilder setProps(Map<String, String> props) {
        fileSchema.setProps(props);
        return this;
    }

    /**
     * get file schema after adding all series and properties
     *
     * @return constructed file schema
     */
    public FileSchema build() {
        return this.fileSchema;
    }
}
