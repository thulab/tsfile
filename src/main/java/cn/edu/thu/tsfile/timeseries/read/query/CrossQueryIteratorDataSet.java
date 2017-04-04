package cn.edu.thu.tsfile.timeseries.read.query;

import java.io.IOException;
import java.util.LinkedHashMap;

import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.read.readSupport.Field;

/**
 * This class is the subClass of {@code QueryDataSet}. It is used to store
 * and fetch more records for batch query in TsFile's SingleFileQuery. 
 * @author Jinrui Zhang
 *
 */

public abstract class CrossQueryIteratorDataSet extends QueryDataSet{
	private static final Logger logger = LoggerFactory.getLogger(CrossQueryIteratorDataSet.class);
	//special for save time values when processing cross getIndex
	private boolean hasReadAll;
	
	public CrossQueryIteratorDataSet(CrossQueryTimeGenerator timeGenerator){
		this.timeQueryDataSet = timeGenerator;
		mapRet = new LinkedHashMap<>();
		try {
			hasReadAll = getMoreRecords();
		} catch (IOException e) {
			logger.error("Error in create CrossQueryIteratorDataSet:", e);
		}
		size = mapRet.size();
	}
	
	/**
	 * 
	 * @return True represents that there is no more data to be read.
	 * @throws IOException
	 */
	public abstract boolean getMoreRecords() throws IOException;
	
	public boolean hasNextRecord(){
		if(!ifInit){
			//That hasReadAll is true represents that there is no records in this QueryDataSet
			if(hasReadAll){
				return false;
			}
			initForRecord();
			ifInit = true;
		}
		if(heap.peek() != null){
			return true;
		}
		if(!hasReadAll){
			try {
				hasReadAll = getMoreRecords();
				if(hasReadAll){
					return false;
				}
				initForRecord();
				if(heap.peek() != null){
					return true;
				}
			} catch (IOException e) {
				logger.error("Error in get Next Record:", e);
			}
		}
		return false;
	}
	
	public RowRecord getNextRecord(){
		if(!hasNextRecord()){
			return null;
		}
		
		Long minTime = heapGet();
		RowRecord r = new RowRecord(minTime, null, null);
		for(int i = 0 ; i < size ; i++){
			if(i == 0){
				r.setDeltaObjectId(deltaObjectIds[i]);
				r.setDeltaObjectType(cols[i].getDeltaObjectType());
			}
			Field f;
			
			//get more fields in columns i
			if(idxs[i] < cols[i].length){
				//Get more fields from file...
			}
			
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
}









