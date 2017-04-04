package cn.edu.thu.tsfile.timeseries.read.query;

import java.util.ArrayList;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;

/**
 * DynamicOneColumnData is a self-defined data structure which is optimized for different type 
 * of values. This class can be viewed as a collection which is more efficient than ArrayList<>. 
 * @author Jinrui Zhang
 *
 */
public class DynamicOneColumnData {
	
	//Read status
	public int rowGroupIndex = 0;
	public long pageOffset = -1;
	public long leftSize = -1;
	public boolean hasReadAll = false;
	public int insertTrueIndex = 0;
	
	public static final int CAPACITY = 10000; 
	
	private String deltaObjectType;
	public TSDataType dataType;
	public String deltaObject;
	public String measurementID;
	
	public int arrayIdx;
	public int valueIdx;
	public int length;
	public int curIdx;
	
	//Some variables that record overflow information
	public DynamicOneColumnData insertTrue;
	public DynamicOneColumnData updateTrue;
	public DynamicOneColumnData updateFalse;
	public SingleSeriesFilterExpression timeFilter;
	//End read status
	
	//Some variables that record time values
	public ArrayList<long[]> timeRet = null;
	public int timeArrayIdx;
	public int timeValueIdx;
	public int timeLength;
	
	public ArrayList<boolean[]> booleanRet;
	public ArrayList<int[]> intRet;
	public ArrayList<long[]> longRet;
	public ArrayList<float[]> floatRet;
	public ArrayList<double[]> doubleRet;
	public ArrayList<Binary[]> binaryRet;
	
	public DynamicOneColumnData(){
		dataType = null;
	}
	
	public DynamicOneColumnData(TSDataType type){
		dataType = type;
	}
	
	/**
	 * 
	 * @param type Data type to record for this DynamicOneColumnData
	 * @param recordTime whether to record time value for this DynamicOneColumnData
	 */
	public DynamicOneColumnData(TSDataType type, boolean recordTime){
		init(type, recordTime);
	}
	
	public void init(TSDataType type, boolean recordTime){
		this.dataType = type;
		this.arrayIdx = 0;
		this.valueIdx = 0;
		this.length = 0;
		this.curIdx = 0;
		
		if(recordTime){
			timeRet = new ArrayList<>();
			timeRet.add(new long[CAPACITY]);
			timeArrayIdx = 0;
			timeValueIdx = 0;
			timeLength = 0;
		}
		
		switch(dataType){
		case BOOLEAN:
			booleanRet = new ArrayList<>();
			booleanRet.add(new boolean[CAPACITY]);
			break;
		case INT32:
			intRet = new ArrayList<>();
			intRet.add(new int[CAPACITY]);
			break;
		case INT64:
			longRet = new ArrayList<>();
			longRet.add(new long[CAPACITY]);
			break;
		case FLOAT:
			floatRet = new ArrayList<>();
			floatRet.add(new float[CAPACITY]);
			break;
		case DOUBLE:
			doubleRet = new ArrayList<>();
			doubleRet.add(new double[CAPACITY]);
			break;
		case BYTE_ARRAY:
			binaryRet = new ArrayList<>();
			binaryRet.add(new Binary[CAPACITY]);
			break;
		default:
			break;
		}
	}
	
	public void putTime(long v){
		if(timeValueIdx == CAPACITY){
			this.timeRet.add(new long[CAPACITY]);
			timeArrayIdx ++;
			timeValueIdx = 0;
		}
		(timeRet.get(timeArrayIdx))[timeValueIdx++] = v;
		timeLength ++;
	}

	public void putTimePair(long s, long e){
		if(timeValueIdx == CAPACITY){
			this.timeRet.add(new long[CAPACITY]);
			timeArrayIdx ++;
			timeValueIdx = 0;
		}
		(timeRet.get(timeArrayIdx))[timeValueIdx++] = s;
		timeLength ++;

		if(timeValueIdx == CAPACITY){
			this.timeRet.add(new long[CAPACITY]);
			timeArrayIdx ++;
			timeValueIdx = 0;
		}
		(timeRet.get(timeArrayIdx))[timeValueIdx++] = e;
		timeLength ++;
	}

	public void initTimeRet(){
		timeRet = new ArrayList<>();
		timeRet.add(new long[CAPACITY]);
		timeArrayIdx = 0;
		timeValueIdx = 0;
		timeLength = 0;
	}
	
	/**
	 * add all time and value from another DynamicOneColumnData to self.
	 * @param col
	 */
	public void mergeRecord(DynamicOneColumnData col){
		for(int i = 0 ; i < col.timeLength ; i++){
			putTime(col.getTime(i));
		}
		switch(dataType){
		case BOOLEAN:
			for(int i = 0 ; i < col.length ; i ++){
				putBoolean(col.getBoolean(i));
			}
			break;
		case INT32:
			for(int i = 0 ; i < col.length ; i ++){
				putInt(col.getInt(i));
			}
			break;
		case INT64:
			for(int i = 0 ; i < col.length ; i ++){
				putLong(col.getLong(i));
			}
			break;
		case FLOAT:
			for(int i = 0 ; i < col.length ; i ++){
				putFloat(col.getFloat(i));
			}
			break;
		case DOUBLE:
			for(int i = 0 ; i < col.length ; i ++){
				putDouble(col.getDouble(i));
			}
			break;
		case BYTE_ARRAY:
			for(int i = 0 ; i < col.length ; i ++){
				putBinary(col.getBinary(i));
			}
			break;
		default:
			break;
		}
	}
	
	public void putBoolean(boolean v){
		if(valueIdx == CAPACITY){
			if(this.booleanRet.size() <= arrayIdx + 1){
				this.booleanRet.add(new boolean[CAPACITY]);
			}
			arrayIdx ++;
			valueIdx = 0;
		}
		(this.booleanRet.get(arrayIdx))[valueIdx++] = v;
		length ++;
	}
	
	public void putInt(int v){
		if(valueIdx == CAPACITY){
			if(this.intRet.size() <= arrayIdx + 1){
				this.intRet.add(new int[CAPACITY]);
			}
			arrayIdx ++;
			valueIdx = 0;
		}
		(this.intRet.get(arrayIdx))[valueIdx++] = v;
		length ++;
	}
	
	public void putLong(long v){
		if(valueIdx == CAPACITY){
			if(this.longRet.size() <= arrayIdx + 1){
				this.longRet.add(new long[CAPACITY]);
			}
			arrayIdx ++;
			valueIdx = 0;
		}
		(this.longRet.get(arrayIdx))[valueIdx++] = v;
		length ++;
	}
	
	public void putFloat(float v){
		if(valueIdx == CAPACITY){
			if(this.floatRet.size() <= arrayIdx + 1){
				this.floatRet.add(new float[CAPACITY]);
			}
			arrayIdx ++;
			valueIdx = 0;
		}
		(this.floatRet.get(arrayIdx))[valueIdx++] = v;
		length ++;
	}
	
	public void putDouble(double v){
		if(valueIdx == CAPACITY){
			if(this.doubleRet.size() <= arrayIdx + 1){
				this.doubleRet.add(new double[CAPACITY]);
			}
			arrayIdx ++;
			valueIdx = 0;
		}
		(this.doubleRet.get(arrayIdx))[valueIdx++] = v;
		length ++;
	}
	
	public void putBinary(Binary v){
		if(valueIdx == CAPACITY){
			if(this.binaryRet.size() <= arrayIdx + 1){
				this.binaryRet.add(new Binary[CAPACITY]);
			}
			arrayIdx ++;
			valueIdx = 0;
		}
		(this.binaryRet.get(arrayIdx))[valueIdx++] = v;
		length ++;
	}
	
	/**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception. 
     */
	private void rangeCheck(int idx) {
		if(idx < 0){
			throw new IndexOutOfBoundsException("Index is negative: " + idx);
		}
        if (idx >= length){
            throw new IndexOutOfBoundsException("Index : " + idx + ". Length : " + length);
        }
    }
	
	/**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception. 
     */
	private void rangeCheckForTime(int idx) {
		if(idx < 0){
			throw new IndexOutOfBoundsException("Index is negative: " + idx);
		}
        if (idx >= timeLength){
            throw new IndexOutOfBoundsException("Index : " + idx + ". Length : " + length);
        }
    }
	
	public boolean getBoolean(int idx){
		rangeCheck(idx);
		return this.booleanRet.get(idx / CAPACITY)[idx % CAPACITY];
	}
	
	public int getInt(int idx){
		rangeCheck(idx);
		return this.intRet.get(idx / CAPACITY)[idx % CAPACITY];
	}
	
	public long getLong(int idx){
		rangeCheck(idx);
		return this.longRet.get(idx / CAPACITY)[idx % CAPACITY];
	}
	
	public float getFloat(int idx){
		rangeCheck(idx);
		return this.floatRet.get(idx / CAPACITY)[idx % CAPACITY];
	}
	
	public double getDouble(int idx){
		rangeCheck(idx);
		return this.doubleRet.get(idx / CAPACITY)[idx % CAPACITY];
	}
	
	public Binary getBinary(int idx){
		rangeCheck(idx);
		return this.binaryRet.get(idx / CAPACITY)[idx % CAPACITY];
	}
	
	public long getTime(int idx){
		rangeCheckForTime(idx);
		return this.timeRet.get(idx / CAPACITY)[idx % CAPACITY];
	}
	
	public long[] getTimeAsArray(){
		long[] res = new long[timeLength];
		for(int i = 0 ; i < timeLength; i++){
			res[i] = timeRet.get(i / CAPACITY)[i % CAPACITY];
		}
		return res;
	}
	
	public String getStringValue(int idx){
		switch(dataType){
		case BOOLEAN:
			return String.valueOf(getBoolean(idx));
		case INT32:
			return String.valueOf(getInt(idx));
		case INT64:
			return String.valueOf(getLong(idx));
		case FLOAT:
			return String.valueOf(getFloat(idx));
		case DOUBLE:
			return String.valueOf(getDouble(idx));
		case BYTE_ARRAY:
			return String.valueOf(getBinary(idx));
		default:
			return null;
		}
	}
	
	public String getStringTimeValuePair(int idx){
		String v;
		switch(dataType){
		case BOOLEAN:
			v =  String.valueOf(getBoolean(idx));
			break;
		case INT32:
			v = String.valueOf(getInt(idx));
			break;
		case INT64:
			v = String.valueOf(getLong(idx));
			break;
		case FLOAT:
			v = String.valueOf(getFloat(idx));
			break;
		case DOUBLE:
			v = String.valueOf(getDouble(idx));
			break;
		case BYTE_ARRAY:
			v = String.valueOf(getBinary(idx));
			break;
		default:
			v = "";
			break;
		}
		String t = String.valueOf(getTime(idx));
		StringBuffer sb = new StringBuffer();
		sb.append(t);
		sb.append("\t");
		sb.append(v);
		return sb.toString();
	}
	
	public void putAValueFromDynamicOneColumnData(DynamicOneColumnData B, int idx){
		switch (dataType) {
		case BOOLEAN:
			putBoolean(B.getBoolean(idx));
			break;
		case INT32:
			putInt(B.getInt(idx));
			break;
		case INT64:
			putLong(B.getLong(idx));
			break;
		case FLOAT:
			putFloat(B.getFloat(idx));
			break;
		case DOUBLE:
			putDouble(B.getDouble(idx));
			break;
		case BYTE_ARRAY:
			putBinary(B.getBinary(idx));
			break;
		default:
			break;
		}
	}
	
	public void rollBack(int size){
		//rollback the length
		length -= size;
		timeLength -= size;
		if(size <= valueIdx){
			valueIdx -= size;
			timeValueIdx -= size;
		}else{
			size -= valueIdx;
			size += CAPACITY;
			while(size > CAPACITY){
				switch (dataType){
				case BOOLEAN:
					booleanRet.remove(arrayIdx);
					break;
				case INT32:
					intRet.remove(arrayIdx);
					break;
				case INT64:
					longRet.remove(arrayIdx);
					break;
				case FLOAT:
					floatRet.remove(arrayIdx);
					break;
				case DOUBLE:
					doubleRet.remove(arrayIdx);
					break;
				case BYTE_ARRAY:
					binaryRet.remove(arrayIdx);
					break;
				default:
					break;
				}
				arrayIdx --;
				timeRet.remove(timeArrayIdx);
				timeArrayIdx --;
				
				size -= CAPACITY;
			}
			valueIdx = CAPACITY - size; 
		}
	}
	
	public void clearData(){
		this.init(dataType, true);
	}
	
	public DynamicOneColumnData sub(int startPos){
		return sub(startPos, this.length - 1);
	}
	
	public DynamicOneColumnData sub(int startPos, int endPos){
		DynamicOneColumnData subRes = new DynamicOneColumnData(dataType, true);
		for(int i = startPos; i <= endPos; i ++){
			subRes.putTime(getTime(i));
			subRes.putAValueFromDynamicOneColumnData(this, i);
		}
		return subRes;
	}
	
	public String getDeltaObjectType() {
		return deltaObjectType;
	}

	public void setDeltaObjectType(String deltaObjectType) {
		this.deltaObjectType = deltaObjectType;
	}
	
	public void putOverflowInfo(DynamicOneColumnData insertTrue, DynamicOneColumnData updateTrue,
			DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter){
		this.insertTrue = insertTrue;
		this.updateTrue = updateTrue;
		this.updateFalse = updateFalse;
		this.timeFilter = timeFilter;
	}
	
	public void copyFetchInfoTo(DynamicOneColumnData oneColRet){
		oneColRet.rowGroupIndex = this.rowGroupIndex;
		oneColRet.pageOffset = this.pageOffset;
		oneColRet.leftSize = this.leftSize;
		oneColRet.hasReadAll = this.hasReadAll;
		oneColRet.insertTrueIndex = this.insertTrueIndex;
		oneColRet.insertTrue = this.insertTrue;
		oneColRet.updateFalse = this.updateFalse;
		oneColRet.updateTrue = this.updateTrue;
		oneColRet.timeFilter = this.timeFilter;
	}
	
	public void plusRowGroupIndexAndInitPageOffset(){
		
		this.rowGroupIndex ++;
		//RowGroupIndex's change means that The pageOffset should be updateTo the value in next RowGroup.
		//But we don't know the value, so set the pageOffset to -1. And we calculate the accuracy value
		//in the reading procedure.
		this.pageOffset = -1;
	}

	public int getRowGroupIndex(){
		return this.rowGroupIndex;
	}
}