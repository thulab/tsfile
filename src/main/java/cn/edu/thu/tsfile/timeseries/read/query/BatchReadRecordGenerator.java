package cn.edu.thu.tsfile.timeseries.read.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;


/**
 * This is class is used for {@code OverflowQueryEngine#readWithoutFilter} and
 * {@code OverflowQueryEngine#readOneColumnValueUseFilter} when batch read.
 * 
 * @author Jinrui Zhang
 *
 */
public abstract class BatchReadRecordGenerator {
	public LinkedHashMap<Path, DynamicOneColumnData> retMap;
	private LinkedHashMap<Path, Boolean> hasMoreRet;
	private int noRetCount;
	private HashMap<Long,Integer> timeMap;
	private PriorityQueue<Long> heap;
	private int fetchSize;

	public BatchReadRecordGenerator(List<Path> paths, int fetchSize) throws ProcessorException, IOException{
		noRetCount = 0;
		retMap = new LinkedHashMap<>();
		hasMoreRet = new LinkedHashMap<>();
		timeMap = new HashMap<>();
		this.fetchSize = fetchSize;
		// init for every Series
		for (Path p : paths) {
			DynamicOneColumnData res = getMoreRecordsForOneColumn(p, null);
			retMap.put(p, res);
			if(res.length == 0){
				hasMoreRet.put(p, false);
				noRetCount ++;
			}else{
				hasMoreRet.put(p, true);
			}
		}
		initHeap();
	}

	public void initHeap(){
		heap = new PriorityQueue<>();
		for(Path p : retMap.keySet()){
			DynamicOneColumnData res = retMap.get(p);
			if(res.curIdx < res.length){
				heapPut(res.getTime(res.curIdx));
			}
		}
	}
	
	private void heapPut(long t){
		if(!timeMap.containsKey(t)){
			heap.add(t);
			timeMap.put(t, 1);
		}
	}
	
	private Long heapGet(){
		Long t = heap.poll();
		timeMap.remove(t);
		return t;
	}
	
	public void clearDataInLastQuery(DynamicOneColumnData res){
		res.clearData();
	}
	
	public abstract DynamicOneColumnData getMoreRecordsForOneColumn(Path p
			, DynamicOneColumnData res) throws ProcessorException, IOException;
	
	public void calculateRecord() throws ProcessorException, IOException {
		int recordCount = 0;
		while(recordCount < fetchSize && noRetCount < retMap.size()){
			Long minTime = heapGet();
			if(minTime == null){
				break;
			}
			for(Path p : retMap.keySet()){
				if(hasMoreRet.get(p)){
					DynamicOneColumnData res = retMap.get(p);
					if(minTime.equals(res.getTime(res.curIdx))){
						res.curIdx ++;
						if(res.curIdx == res.length){
							res = getMoreRecordsForOneColumn(p, res);
							if(res.curIdx == res.length){
								hasMoreRet.put(p, false);
								noRetCount ++;
								continue;
							}
						}
						heapPut(res.getTime(res.curIdx));
					}
				}
			}
			recordCount ++;
		}
	}
}














