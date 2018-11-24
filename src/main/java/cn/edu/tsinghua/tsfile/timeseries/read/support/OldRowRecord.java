package cn.edu.tsinghua.tsfile.timeseries.read.support;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.*;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to store one Row-Record<br>
 * All query results can be transformed to this format
 *
 * @author Jinrui Zhang
 */
public class OldRowRecord {
    public long timestamp;
    public String deltaObjectId;
    public List<FieldV1> fieldV1s;

    public OldRowRecord(long timestamp, String deltaObjectId, String deltaObjectType) {
        this.timestamp = timestamp;
        this.deltaObjectId = deltaObjectId;
        this.fieldV1s = new ArrayList<FieldV1>();
    }

    public long getTime() {
        return timestamp;
    }

    public String getRowKey() {
        return deltaObjectId;
    }


    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setDeltaObjectId(String did) {
        this.deltaObjectId = did;
    }

    public int addField(FieldV1 f) {
        this.fieldV1s.add(f);
        return fieldV1s.size();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp);
        for (FieldV1 f : fieldV1s) {
            sb.append("\t");
            sb.append(f);
        }
        return sb.toString();
    }

    public TSRecord toTSRecord() {
        TSRecord r = new TSRecord(timestamp, deltaObjectId);
        for (FieldV1 f : fieldV1s) {
            if (!f.isNull()) {
                DataPoint d = createDataPoint(f.dataType, f.measurementId, f);
                r.addTuple(d);
            }
        }
        return r;
    }

    private DataPoint createDataPoint(TSDataType dataType, String measurementId, FieldV1 f) {
        switch (dataType) {

            case BOOLEAN:
                return new BooleanDataPoint(measurementId, f.getBoolV());
            case DOUBLE:
                return new DoubleDataPoint(measurementId, f.getDoubleV());
            case FLOAT:
                return new FloatDataPoint(measurementId, f.getFloatV());
            case INT32:
                return new IntDataPoint(measurementId, f.getIntV());
            case INT64:
                return new LongDataPoint(measurementId, f.getLongV());
            case TEXT:
                return new StringDataPoint(measurementId, Binary.valueOf(f.getStringValue()));
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    /**
     * @return the fields
     */
    public List<FieldV1> getFieldV1s() {
        return fieldV1s;
    }
}
