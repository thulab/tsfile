package cn.edu.thu.tsfile.timeseries.read.query;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;

import cn.edu.thu.tsfile.timeseries.read.readSupport.Field;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;


public class QueryDataSet {
	private static final char PATH_SPLITER = '.';
	
	//Time Generator for Cross Query when using batching read
	protected CrossQueryTimeGenerator timeQueryDataSet;
	private BatchReadRecordGenerator batchReaderRetGenerator;
	
	//special for save time values when processing cross getIndex
	protected PriorityQueue<Long> heap;
	protected DynamicOneColumnData[] cols;
	protected String[] deltaObjectIds;
	protected String[] measurementIds;
	protected int[] idxs;
	protected HashMap<Long,Integer> timeMap;
	protected int size;
	protected boolean ifInit = false;
	protected RowRecord currentRecord = null;
	
	public LinkedHashMap<String,DynamicOneColumnData> mapRet;
	
	public QueryDataSet(){
		mapRet = new LinkedHashMap<>();
	}
	
	public void initForRecord(){
		size = mapRet.keySet().size();

		heap = new PriorityQueue<>(size);
		cols = new DynamicOneColumnData[size];
		deltaObjectIds = new String[size];
		measurementIds = new String[size];
		idxs = new int[size];
		timeMap = new HashMap<>();
		
		int i = 0;
		for(String key : mapRet.keySet()){
			cols[i] = mapRet.get(key);
			deltaObjectIds[i] = key.substring(0, key.lastIndexOf(PATH_SPLITER));
			measurementIds[i] = key.substring(key.lastIndexOf(PATH_SPLITER) + 1);
			idxs[i] = 0;
			
			if(cols[i] != null && (cols[i].length > 0 || cols[i].timeLength > 0)){
				heapPut(cols[i].getTime(0));
			}
			i ++;
		}
	}
	
	protected void heapPut(long t){
		if(!timeMap.containsKey(t)){
			heap.add(t);
			timeMap.put(t, 1);
		}
	}
	
	protected Long heapGet(){
		Long t = heap.poll();
		timeMap.remove(t);
		return t;
	}
	
	public boolean hasNextRecord(){
		if(!ifInit){
			initForRecord();
			ifInit = true;
		}
		if(heap.peek() != null){
			return true;
		}
		return false;
	}
	
	public RowRecord getNextRecord(){
		if(!ifInit){
			initForRecord();
			ifInit = true;
		}
		
		Long minTime = heapGet();
		if(minTime == null){
			return null;
		}
		
		RowRecord r = new RowRecord(minTime, null, null);
		for(int i = 0 ; i < size ; i++){
			if(i == 0){
				r.setDeltaObjectId(deltaObjectIds[i]);
				r.setDeltaObjectType(cols[i].getDeltaObjectType());
			}
			Field f;
			
			if(idxs[i] < cols[i].length && minTime == cols[i].getTime(idxs[i])){
				f = new Field(cols[i].dataType, deltaObjectIds[i], measurementIds[i]);
				f.setNull(false);
				putValueToField(cols[i], idxs[i], f);
				idxs[i] ++;
				if(idxs[i] < cols[i].length){
					heapPut(cols[i].getTime(idxs[i]));
				}
			}else{
				f = new Field(cols[i].dataType, measurementIds[i]);
				f.setNull(true);
			}
			r.addField(f);
		}
		return r;
	}
	
	public boolean next() {
		if (hasNextRecord()) {
			currentRecord = getNextRecord();
			return true;
		}
		currentRecord = null;
		return false;
	}
	
	public RowRecord getCurrentRecord(){
		return currentRecord;
	}
	
	public void putValueToField(DynamicOneColumnData col, int idx, Field f){
		switch(col.dataType){
		case BOOLEAN:
			f.setBoolV(col.getBoolean(idx));
			break;
		case INT32:
			f.setIntV(col.getInt(idx));
			break;
		case INT64:
			f.setLongV(col.getLong(idx));
			break;
		case FLOAT:
			f.setFloatV(col.getFloat(idx));
			break;
		case DOUBLE:
			f.setDoubleV(col.getDouble(idx));
			break;
		case BYTE_ARRAY:
			f.setBinaryV(col.getBinary(idx));
			break;
		default:
			break;
		}
	}
	
	public void clear(){
		this.ifInit = false;
		for(DynamicOneColumnData col : mapRet.values()){
			col.clearData();
		}
	}
	
	public void putARowRecord(RowRecord record){
		for(Field f : record.fields){
			StringBuilder sb = new StringBuilder();
			sb.append(f.deltaObjectId);
			sb.append(".");
			sb.append(f.measurementId);
			String key = sb.toString();
			if(!mapRet.containsKey(key)){
				DynamicOneColumnData oneCol = new DynamicOneColumnData(f.dataType, true);
				oneCol.setDeltaObjectType(record.deltaObjectType);
				mapRet.put(key, oneCol);
			}			
			switch (f.dataType) {
			case BOOLEAN:
				mapRet.get(key).putBoolean(f.getBoolV());
				break;
			case INT32:
				mapRet.get(key).putInt(f.getIntV());
				break;
			case INT64:
				mapRet.get(key).putLong(f.getLongV());
				break;
			case FLOAT:
				mapRet.get(key).putFloat(f.getFloatV());
				break;
			case DOUBLE:
				mapRet.get(key).putDouble(f.getFloatV());
				break;
			case BYTE_ARRAY:
				mapRet.get(key).putBinary(f.getBinaryV());
				break;
			default:
				break;
			}
			mapRet.get(key).putTime(record.timestamp);
		}
	}
	
	public void putRecordFromBatchReadRetGenerator(){
		for(Path p : getBatchReaderRetGenerator().retMap.keySet()){
			DynamicOneColumnData oneColRet = getBatchReaderRetGenerator().retMap.get(p);
			DynamicOneColumnData leftRet = oneColRet.sub(oneColRet.curIdx);
			leftRet.setDeltaObjectType(oneColRet.getDeltaObjectType());
			//Copy batch read info from oneColRet to leftRet
			oneColRet.copyFetchInfoTo(leftRet);
			getBatchReaderRetGenerator().retMap.put(p, leftRet);
			oneColRet.rollBack(oneColRet.length - oneColRet.curIdx);
			this.mapRet.put(p.getFullPath(), oneColRet);
		}
	}

	public BatchReadRecordGenerator getBatchReaderRetGenerator() {
		return batchReaderRetGenerator;
	}

	public void setBatchReaderRetGenerator(BatchReadRecordGenerator batchReaderRetGenerator) {
		this.batchReaderRetGenerator = batchReaderRetGenerator;
	}
}









