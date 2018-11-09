package cn.edu.tsinghua.tsfile.timeseries.read.query;

import java.util.ArrayList;

public class QueryConfig {

    /** filter for time in String format **/
    private String timeFilter;
    /** filter for frequency in String format **/
    private String freqFilter;
    /** selected column names **/
    private ArrayList<String> selectColumns;
    /** filter for value in String format **/
    private String valueFilter;
    /** query type **/
    private QueryType queryType;

    /**
     * Construct a queryConfig for QUERY_WITHOUT_FILTER
     *
     * @param selects selected columns, split by |
     */
    public QueryConfig(String selects) {
        // init selected columns
        this.selectColumns = new ArrayList<>();
        String[] cols = selects.split("\\|");
        for (String col : cols) {
            selectColumns.add(col);
        }
        // init query type
        this.queryType = QueryType.QUERY_WITHOUT_FILTER;
    }

    /**
     * Construct a queryConfig automatically according to the filters
     *
     * @param selects selected columns
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param valueFilter value filter
     */
    public QueryConfig(String selects, String timeFilter, String freqFilter,
                       String valueFilter) {
        // init selected columns
        this.selectColumns = new ArrayList<String>();
        String[] cols = selects.split("\\|");
        for (String col : cols) {
            selectColumns.add(col);
        }

        // init filters
        this.setTimeFilter(timeFilter);
        this.setFreqFilter(freqFilter);
        this.setValueFilter(valueFilter);

        // init query type
        if (timeFilter.equals("null") && freqFilter.equals("null") && valueFilter.equals("null")) {
            this.queryType = QueryType.QUERY_WITHOUT_FILTER;
        } else if (valueFilter.startsWith("[")) {
            this.queryType = QueryType.CROSS_QUERY;
        } else {
            this.queryType = QueryType.SELECT_ONE_COL_WITH_FILTER;
        }
    }

    /**
     * Construct a queryConfig with selected columns and filters
     *
     * @param selectColumns selected columns
     * @param timeFilter time filter
     * @param freqFilter frequency filter
     * @param valueFilter value filter
     */
    public QueryConfig(ArrayList<String> selectColumns, String timeFilter, String freqFilter,
                       String valueFilter) {
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

    private void setFreqFilter(String freqFilter) {
        this.freqFilter = freqFilter;
    }

    public String getValueFilter() {
        return valueFilter;
    }

    private void setValueFilter(String valueFilter) {
        this.valueFilter = valueFilter;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }


}
