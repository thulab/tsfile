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
    /** time stamp of this record **/
    public long timestamp;
    /** delta object ID of this record **/
    public String deltaObjectId;
    /** all value fields of this record **/
    public List<Field> fields;

    /**
     * set timestamp and deltaObjectId
     * @param timestamp
     * @param deltaObjectId
     * @param deltaObjectType
     */
    public OldRowRecord(long timestamp, String deltaObjectId, String deltaObjectType) {
        this.timestamp = timestamp;
        this.deltaObjectId = deltaObjectId;
        this.fields = new ArrayList<Field>();
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

    /**
     * add one value field
     * @param f
     * @return
     */
    public int addField(Field f) {
        this.fields.add(f);
        return fields.size();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp);
        for (Field f : fields) {
            sb.append("\t");
            sb.append(f);
        }
        return sb.toString();
    }

    /**
     * convert this format of record to TSRecord
     * @return
     */
    public TSRecord toTSRecord() {
        TSRecord r = new TSRecord(timestamp, deltaObjectId);
        for (Field f : fields) {
            if (!f.isNull()) {
                DataPoint d = createDataPoint(f.dataType, f.measurementId, f);
                r.addTuple(d);
            }
        }
        return r;
    }

    /**
     * convert {@code Field} to {@code DataPoint}
     * @param dataType
     * @param measurementId
     * @param f
     * @return
     */
    private DataPoint createDataPoint(TSDataType dataType, String measurementId, Field f) {
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
    public List<Field> getFields() {
        return fields;
    }
}
