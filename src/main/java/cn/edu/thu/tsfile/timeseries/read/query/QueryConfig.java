package cn.edu.thu.tsfile.timeseries.read.query;

import java.util.ArrayList;

public class QueryConfig {
	
	private String timeFilter;
	private String freqFilter;
	private ArrayList<String> selectColumns;
	private String valueFilter;
	private QueryType queryType;
	
	public QueryConfig(){
		this.selectColumns = new ArrayList<>();
	}
	
	/**
	 * Construct a queryConfig for QUERY_WITHOUT_FILTER
	 * @param s
	 */
	public QueryConfig(String s){
		this.selectColumns = new ArrayList<String>();
		String[] cols = s.split("\\|");
		for(String col : cols){
			selectColumns.add(col);
		}
		this.queryType = QueryType.QUERY_WITHOUT_FILTER;
	}
	
	/**
	 * Construct a queryConfig automatically according to the filters
	 * @param s
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 */
	public QueryConfig(String s, String timeFilter, String freqFilter,
			 String valueFilter){
		this.selectColumns = new ArrayList<String>();
		String[] cols = s.split("\\|");
		
		for(String col : cols){
			selectColumns.add(col);
		}
		
		this.setTimeFilter(timeFilter);
		this.setFreqFilter(freqFilter);
		this.setValueFilter(valueFilter);
		
		if(timeFilter.equals("null") && freqFilter.equals("null") && valueFilter.equals("null")){
			this.queryType = QueryType.QUERY_WITHOUT_FILTER;
		}else if(valueFilter.startsWith("[")){
			this.queryType = QueryType.CROSS_QUERY;
		}else{
			this.queryType = QueryType.SELECT_ONE_COL_WITH_FILTER;
		}
	}
	
	public QueryConfig(ArrayList<String> selectColumns, String timeFilter, String freqFilter,
			 String valueFilter){
		this.selectColumns = selectColumns;
		this.setTimeFilter(timeFilter);
		this.setFreqFilter(freqFilter);
		this.setValueFilter(valueFilter);
	}

	
	public ArrayList<String> getSelectColumns() {
		return selectColumns;
	}

	public void setSelectColumns(ArrayList<String> selectColumns) {
		this.selectColumns = selectColumns;
	}

	public String getTimeFilter() {
		return timeFilter;
	}

	public void setTimeFilter(String timeFilter) {
		this.timeFilter = timeFilter;
	}

	public String getFreqFilter() {
		return freqFilter;
	}

	public void setFreqFilter(String freqFilter) {
		this.freqFilter = freqFilter;
	}

	public String getValueFilter() {
		return valueFilter;
	}

	public void setValueFilter(String valueFilter) {
		this.valueFilter = valueFilter;
	}

	public QueryType getQueryType() {
		return queryType;
	}

	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}


	
}
