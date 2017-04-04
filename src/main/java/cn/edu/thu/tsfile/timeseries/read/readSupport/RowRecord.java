package cn.edu.thu.tsfile.timeseries.read.readSupport;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.record.datapoint.DoubleDataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.datapoint.BooleanDataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.datapoint.LongDataPoint;

/**
 * This class is used to store one Row-Record<br>
 * All query results can be transformed to this format
 * @author Jinrui Zhang
 *
 */
public class RowRecord {
	public long timestamp;
	public String deltaObjectId;
	public String deltaObjectType;
	public List<Field> fields;

	public RowRecord(long timestamp, String deltaObjectId, String deltaObjectType) {
		this.timestamp = timestamp;
		this.deltaObjectType = deltaObjectType;
		this.deltaObjectId = deltaObjectId;
		this.fields = new ArrayList<Field>();
	}

	public long getTime() {
		return timestamp;
	}

	public String getRowKey() {
		return deltaObjectId;
	}

	public String getDeltaObjectType() {
		return deltaObjectType;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setDeltaObjectId(String did) {
		this.deltaObjectId = did;
	}

	public void setDeltaObjectType(String deltaObjecttype) {
		this.deltaObjectType = deltaObjecttype;
	}

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

	public TSRecord toTSRecord() {
		TSRecord r = new TSRecord(timestamp, deltaObjectId);
		for (Field f : fields) {
			DataPoint d = createDataPoint(f.dataType, f.measurementId, f);
			r.addTuple(d);
		}
		return r;
	}

	public DataPoint createDataPoint(TSDataType dataType, String measurementId, Field f){
		switch(dataType){
		
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
		default:
			return null;
		}
	}
	
	/**
	 * @return the fields
	 */
	public List<Field> getFields() {
		return fields;
	}

}
