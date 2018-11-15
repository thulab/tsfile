package cn.edu.tsinghua.tsfile.timeseries.read.datatype;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class RowRecord {
    /** time stamp of this record **/
    private long timestamp;
    /** all value fields of this record **/
    private LinkedHashMap<Path, TsPrimitiveType> fields;

    /**
     * init this.fields
     */
    private RowRecord() {
        fields = new LinkedHashMap<>();
    }

    /**
     * init this.fields and set time stamp
     * @param timestamp
     */
    public RowRecord(long timestamp) {
        this();
        this.timestamp = timestamp;
    }

    /**
     * add one <path, field>
     * @param path
     * @param tsPrimitiveType
     */
    public void putField(Path path, TsPrimitiveType tsPrimitiveType) {
        fields.put(path, tsPrimitiveType);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public LinkedHashMap<Path, TsPrimitiveType> getFields() {
        return fields;
    }

    public void setFields(LinkedHashMap<Path, TsPrimitiveType> fields) {
        this.fields = fields;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("[Timestamp]:").append(timestamp);
        for (Map.Entry<Path, TsPrimitiveType> entry : fields.entrySet()) {
            stringBuilder.append("\t[").append(entry.getKey()).append("]:").append(entry.getValue());
        }
        return stringBuilder.toString();
    }
}
