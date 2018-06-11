package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * For more information, see TimeSeries in cn.edu.thu.tsfile.format package
 */
public class TimeSeriesMetadata {

    private String measurementUID;

    private TSDataType type;

    public TimeSeriesMetadata() {
    }

    public TimeSeriesMetadata(String measurementUID, TSDataType dataType) {
        this.measurementUID = measurementUID;
        this.type = dataType;
    }

    public String getMeasurementUID() {
        return measurementUID;
    }

    public void setMeasurementUID(String measurementUID) {
        this.measurementUID = measurementUID;
    }

    public TSDataType getType() {
        return type;
    }

    public void setType(TSDataType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("TimeSeriesMetadata: measurementUID %s, DataType %s", measurementUID, type);
    }
}
