package cn.edu.thu.tsfile.timeseries.read.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.CSAnd;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.CSOr;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;

/**
 * This class is used in batch query for Cross Query.
 * @author Jinrui Zhang
 */
public abstract class CrossQueryTimeGenerator {
	
	protected SingleSeriesFilterExpression timeFilter;
	protected SingleSeriesFilterExpression freqFilter;
	protected FilterExpression valueFilter;
	public ArrayList<DynamicOneColumnData> retMap;
//	HashMap<String, SingleSeriesFilterExpression> filterMap;
	public ArrayList<Boolean> hasReadAllMap;
	protected ArrayList<Long> lastValueMap;
	protected ArrayList<Integer> idxCount;
	protected int fetchSize;
	//to record which valueFilter is used
	protected int dfsCnt; 
	
	public CrossQueryTimeGenerator(SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, 
			FilterExpression valueFilter, int fetchSize){
		retMap = new ArrayList<>();
		hasReadAllMap = new ArrayList<>();
		lastValueMap = new ArrayList<>();
		idxCount = new ArrayList<>();
		this.valueFilter = valueFilter;
		this.timeFilter = timeFilter;
		this.fetchSize = fetchSize;
		dfsCnt = -1;
		initRetMapAndFilterMap(valueFilter);
	}
	
	private int initRetMapAndFilterMap(FilterExpression valueFilter){
		dfsCnt ++;
		int tmpIdx = dfsCnt;
		retMap.add(null);
		hasReadAllMap.add(false);
		lastValueMap.add(-1L);
		idxCount.add(-1);
		
		if(valueFilter instanceof SingleSeriesFilterExpression){
			idxCount.set(tmpIdx, 1);
			return 1;
			
		}else if(valueFilter instanceof CSAnd){
			FilterExpression left = ((CSAnd) valueFilter).getLeft();
			FilterExpression right = ((CSAnd) valueFilter).getRight();
			int l = initRetMapAndFilterMap(left);
			int r = initRetMapAndFilterMap(right);
			idxCount.set(tmpIdx, l + r + 1);
			return l + r + 1;
			
		// else if(valueFilter instanceof CSOr)
		}else{
			FilterExpression left = ((CSOr) valueFilter).getLeft();
			FilterExpression right = ((CSOr) valueFilter).getRight();
			int l = initRetMapAndFilterMap(left);
			int r = initRetMapAndFilterMap(right);
			idxCount.set(tmpIdx, l + r + 1);
			return l + r + 1;
		}
	}
	
	public DynamicOneColumnData getMoreRecordForOneCol(int idx, SingleSeriesFilterExpression valueFilter) 
			throws ProcessorException, IOException{
		DynamicOneColumnData res = retMap.get(idx);
		if (res != null) {
			res.clearData();
		}
		res = getDataInNextBatch(res, fetchSize, valueFilter);
		retMap.set(idx, res);
		if(res == null || res.length == 0){
			hasReadAllMap.set(idx, true);
		}
		return res;
	}
	
	public abstract DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize
			, SingleSeriesFilterExpression valueFilter) throws ProcessorException, IOException;
	
	public long[] generateTimes() throws ProcessorException, IOException{
		long[] res = new long[fetchSize];
		
		int cnt = 0;
		SingleValueVisitor<Long> timeFilterVisitor = new SingleValueVisitor<Long>();
		while(cnt < fetchSize){
			//must before calculateOneTime
			dfsCnt = -1; 
			long v = calculateOneTime(valueFilter);
			if(v == -1){
				break;
			}
			if((timeFilter == null) || (timeFilter != null && timeFilterVisitor.satisfy(v, timeFilter))){
				res[cnt] = v;
				cnt ++;
			}
		}
		if(cnt < fetchSize){
			return Arrays.copyOfRange(res, 0, cnt);
		}
		return res;
	}
	
	private long calculateOneTime(FilterExpression valueFilter) throws ProcessorException, IOException{
		//first check whether has a value not used in CSOr
		dfsCnt ++;
		if(lastValueMap.get(dfsCnt) != -1L){
			long v = lastValueMap.get(dfsCnt);
			lastValueMap.set(dfsCnt, -1L);
			dfsCnt += (idxCount.get(dfsCnt) - 1);
			return v;
		}
		if(valueFilter instanceof SingleSeriesFilterExpression){
			DynamicOneColumnData res = retMap.get(dfsCnt);
			
			if((res == null) || (res.curIdx == res.length && !hasReadAllMap.get(dfsCnt))){
				res = getMoreRecordForOneCol(dfsCnt, (SingleSeriesFilterExpression)valueFilter);
			}
			if(res == null || res.curIdx == res.length){
				//represent this col has no more value
				return -1;
			}
			return res.getTime(res.curIdx++);
		}else if(valueFilter instanceof CSAnd){
			FilterExpression left = ((CSAnd) valueFilter).getLeft();
			FilterExpression right = ((CSAnd) valueFilter).getRight();
			int leftPreIndex = dfsCnt;
			long l = calculateOneTime(left);
			int rightPreIndex = dfsCnt;
			long r = calculateOneTime(right);
			while(l != -1 && r != -1){
				while(l < r && l != -1){
					dfsCnt = leftPreIndex;
					l = calculateOneTime(left);
				}
				if(l == r){
					break;
				}
				dfsCnt = rightPreIndex;
				r = calculateOneTime(right);
			}
			if(l == -1 || r == -1){
				return -1;
			}
			return l;
		}else if(valueFilter instanceof CSOr){
			FilterExpression left = ((CSOr) valueFilter).getLeft();
			FilterExpression right = ((CSOr) valueFilter).getRight();
			int lidx = dfsCnt + 1;
			long l = calculateOneTime(left);
			int ridx = dfsCnt + 1;
			long r = calculateOneTime(right);
			
			if(l == -1 && r != -1){
				return r;
			}else if(l != -1 && r == -1){
				return l;
			}else if(l == -1 && r == -1){
				return -1;
			}else{
				if(l < r){
					lastValueMap.set(ridx, r);
					return l;
				}else if(l > r){
					lastValueMap.set(lidx, l);
					return r;
				}else{
					return l;
				}
			}
		}
		return -1;
	}
}

