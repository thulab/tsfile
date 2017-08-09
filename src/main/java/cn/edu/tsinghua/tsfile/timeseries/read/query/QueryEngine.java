package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.constant.QueryConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.FilterUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.LocalFileInput;
import cn.edu.tsinghua.tsfile.timeseries.read.RecordReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.metadata.SeriesSchema;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class QueryEngine {
    private static final Logger logger = LoggerFactory.getLogger(QueryEngine.class);
    private static int FETCH_SIZE = 20000;
    public TSRandomAccessFileReader raf;
    private RecordReader recordReader;

    public QueryEngine(TSRandomAccessFileReader raf) throws IOException {
        this.raf = raf;
        recordReader = new RecordReader(raf);
    }

    public QueryEngine(TSRandomAccessFileReader raf, int fetchSize) throws IOException {
        this.raf = raf;
        recordReader = new RecordReader(raf);
        FETCH_SIZE = fetchSize;
    }

    public static QueryDataSet query(QueryConfig config, String fileName) throws IOException {
        LocalFileInput raf = new LocalFileInput(fileName);
        QueryEngine queryEngine = new QueryEngine(raf);
        QueryDataSet queryDataSet = queryEngine.query(config);
        raf.close();
        return queryDataSet;
    }

    /**
     * Get All Column info for every deltaObject
     *
     * @param raf
     * @return
     * @throws IOException
     */
    public static HashMap<String, ArrayList<SeriesSchema>> getAllColumns(TSRandomAccessFileReader raf) throws IOException {
        RecordReader recordReader = new RecordReader(raf);
        return recordReader.getAllSeriesSchemasGroupByDeltaObject();
    }

    /**
     * Get RowGroupSize for every deltaObject
     *
     * @param raf
     * @return HashMap
     * @throws IOException
     */
    public static HashMap<String, Integer> getDeltaObjectRowGroupCount(TSRandomAccessFileReader raf) throws IOException {
        RecordReader recordReader = new RecordReader(raf);
        return recordReader.getDeltaObjectRowGroupCounts();
    }

    public static HashMap<String, String> getDeltaObjectTypes(TSRandomAccessFileReader raf) throws IOException {
        RecordReader recordReader = new RecordReader(raf);
        return recordReader.getDeltaObjectTypes();
    }

    public QueryDataSet query(QueryConfig config) throws IOException {
        if (config.getQueryType() == QueryType.QUERY_WITHOUT_FILTER) {
            return readWithoutFilter(config);
        } else if (config.getQueryType() == QueryType.SELECT_ONE_COL_WITH_FILTER) {
            return readOneColumnValueUseFilter(config);
        } else if (config.getQueryType() == QueryType.CROSS_QUERY) {
            return crossColumnQuery(config);
        }
        return null;
    }

    public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                              FilterExpression valueFilter) throws IOException {

        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return readWithoutFilter(paths);
        } else if (valueFilter instanceof SingleSeriesFilterExpression || (timeFilter != null && valueFilter == null)) {
            return readOneColumnValueUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter);
        } else if (valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter);
        }
        return null;
    }

    public QueryDataSet query(QueryConfig config, Map<String, Long> params) throws IOException {
        List<Path> paths = getPathsFromSelectedColumns(config.getSelectColumns());

        SingleSeriesFilterExpression timeFilter = FilterUtils.construct(config.getTimeFilter(), null);
        SingleSeriesFilterExpression freqFilter = FilterUtils.construct(config.getFreqFilter(), null);
        FilterExpression valueFilter;
        if (config.getQueryType() == QueryType.CROSS_QUERY) {
            valueFilter = FilterUtils.constructCrossFilter(config.getValueFilter(), recordReader);
        } else {
            valueFilter = FilterUtils.construct(config.getValueFilter(), recordReader);
        }
        return query(paths, timeFilter, freqFilter, valueFilter, params);
    }

    public QueryDataSet query(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter,
                              FilterExpression valueFilter, Map<String, Long> params) throws IOException {

        long startOffset = params.get(QueryConstant.PARTITION_START_OFFSET);
        long endOffset = params.get(QueryConstant.PARTITION_END_OFFSET);

        ArrayList<Integer> idxs = calSpecificRowGroupByPartition(startOffset, endOffset);

        if (logger.isDebugEnabled()) {
            logger.debug(startOffset + "|" + endOffset + "|" + idxs);
        }
        return queryWithSpecificRowGroups(paths, timeFilter, freqFilter, valueFilter, idxs);
    }

    private List<Path> getPathsFromSelectedColumns(List<String> selectedColumns) {
        List<Path> paths = new ArrayList<>();
        for (String s : selectedColumns) {
            Path p = new Path(s);
            paths.add(p);
        }
        return paths;
    }

    /**
     * read from specific RowGroup
     *
     * @param config
     * @param idx    The index of RowGroup for given deltaObject in config
     * @return QueryDataSet
     * @throws IOException
     */
    public QueryDataSet queryInOneRowGroup(QueryConfig config, int idx) throws IOException {
        List<Path> paths = getPathsFromSelectedColumns(config.getSelectColumns());
        SingleSeriesFilterExpression timeFilter = FilterUtils.construct(config.getTimeFilter(), null);
        SingleSeriesFilterExpression freqFilter = FilterUtils.construct(config.getFreqFilter(), null);
        FilterExpression valueFilter;
        if (config.getQueryType() == QueryType.CROSS_QUERY) {
            valueFilter = FilterUtils.constructCrossFilter(config.getValueFilter(), recordReader);
        } else {
            valueFilter = FilterUtils.construct(config.getValueFilter(), recordReader);
        }

        ArrayList<Integer> rowGroupIndexList = new ArrayList<>();
        rowGroupIndexList.add(idx);
        return queryWithSpecificRowGroups(paths, timeFilter, freqFilter, valueFilter, rowGroupIndexList);
    }

    private QueryDataSet queryWithSpecificRowGroups(List<Path> paths, FilterExpression timeFilter, FilterExpression freqFilter
            , FilterExpression valueFilter, ArrayList<Integer> rowGroupIndexList) throws IOException {
        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return readWithoutFilter(paths, rowGroupIndexList);
        } else if (valueFilter instanceof SingleSeriesFilterExpression || (timeFilter != null && valueFilter == null)) {
            return readOneColumnValueUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (SingleSeriesFilterExpression) valueFilter, rowGroupIndexList);
        } else if (valueFilter instanceof CrossSeriesFilterExpression) {
            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
                    (CrossSeriesFilterExpression) valueFilter, rowGroupIndexList);
        }
        throw new IOException("Query Not Support Exception");
    }

    private QueryDataSet readWithoutFilter(QueryConfig config) throws IOException {
        List<Path> paths = getPathsFromSelectedColumns(config.getSelectColumns());
        return readWithoutFilter(paths);
    }

    /**
     * QueryWithoutFilter #1 : Query without filter according to paths
     *
     * @param paths path list which need to be selected
     * @return QueryDataSet
     * @throws IOException
     */
    private QueryDataSet readWithoutFilter(List<Path> paths) throws IOException {
        return new IteratorQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValueInOneColumn(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString());
            }
        };
    }

    /**
     * QueryWithoutFilter #2 : Query without filter according to paths and specific RowGroup(s)
     *
     * @param paths           path list which need to be selected
     * @param RowGroupIdxList RowGroup index list.
     * @throws IOException
     */
    public QueryDataSet readWithoutFilter(List<Path> paths, ArrayList<Integer> RowGroupIdxList) throws IOException {
        return new IteratorQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValueInOneColumn(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString(), RowGroupIdxList);
            }
        };
    }

    /**
     * QueryWithSingleSeriesFilterExpression #1: aimed to getIndex with filter, BUT only one column allowed
     *
     * @param config
     * @return
     * @throws IOException
     */
    public QueryDataSet readOneColumnValueUseFilter(QueryConfig config) throws IOException {
        SingleSeriesFilterExpression timeFilter = FilterUtils.construct(config.getTimeFilter(), null);
        SingleSeriesFilterExpression freqFilter = FilterUtils.construct(config.getFreqFilter(), null);
        SingleSeriesFilterExpression valueFilter = FilterUtils.construct(config.getValueFilter(), recordReader);
        List<Path> paths = getPathsFromSelectedColumns(config.getSelectColumns());
        return readOneColumnValueUseFilter(paths, timeFilter, freqFilter, valueFilter);
    }

    /**
     * QueryWithSingleSeriesFilterExpression #2: aimed to getIndex with filter, but only one column allowed
     * only one column allowed
     *
     * @param paths
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @return
     * @throws IOException
     */
    private QueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                     SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        logger.debug("start read one column data with filter...");
        return new IteratorQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValuesUseFilter(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString()
                        , timeFilter, freqFilter, valueFilter);
            }
        };
    }

    /**
     * QueryWithSingleSeriesFilterExpression #3: aimed to getIndex with filter according to paths from
     * specific RowGroups, BUT only one column allowed
     *
     * @param paths
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @return QueryDataSet
     * @throws IOException
     */
    public QueryDataSet readOneColumnValueUseFilter(List<Path> paths, SingleSeriesFilterExpression timeFilter,
                                                    SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter, ArrayList<Integer> rowGroupIndexList) throws IOException {
        logger.debug("start read one column data with filter according to specific RowGroup Index List {}", rowGroupIndexList);

        return new IteratorQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValuesUseFilter(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString()
                        , timeFilter, freqFilter, valueFilter, rowGroupIndexList);
            }
        };
    }

    /**
     * CrossQuery #1: Function for Cross Columns Query
     *
     * @param config
     * @return QueryDataSet
     * @throws IOException
     */
    private QueryDataSet crossColumnQuery(QueryConfig config) throws IOException {
        logger.info("start cross columns getIndex...");
        SingleSeriesFilterExpression timeFilter = FilterUtils.construct(config.getTimeFilter(), null);
        SingleSeriesFilterExpression freqFilter = FilterUtils.construct(config.getFreqFilter(), null);
        CrossSeriesFilterExpression valueFilter = (CrossSeriesFilterExpression) FilterUtils.constructCrossFilter(config.getValueFilter(),
                recordReader);
        List<Path> paths = getPathsFromSelectedColumns(config.getSelectColumns());
        return crossColumnQuery(paths, timeFilter, freqFilter, valueFilter);
    }

    /**
     * CrossQuery #2: Function for Cross Columns Query
     *
     * @param paths
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @return QueryDataSet
     * @throws IOException
     */
    private QueryDataSet crossColumnQuery(List<Path> paths, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                          CrossSeriesFilterExpression valueFilter) throws IOException {

        CrossQueryTimeGenerator timeGenerator = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, FETCH_SIZE) {
            @Override
            public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                           SingleSeriesFilterExpression valueFilter) throws ProcessorException, IOException {
                return recordReader.getValuesUseFilter(res, fetchSize, valueFilter);
            }
        };

        return new CrossQueryIteratorDataSet(timeGenerator) {
            @Override
            public boolean getMoreRecords() throws IOException {
                try {
                    long[] timeRet = timeQueryDataSet.generateTimes();
                    if (timeRet.length == 0) {
                        return true;
                    }
                    for (Path p : paths) {
                        String deltaObjectUID = p.getDeltaObjectToString();
                        String measurementUID = p.getMeasurementToString();
                        DynamicOneColumnData oneColDataList = recordReader.getValuesUseTimeValue(deltaObjectUID, measurementUID, timeRet);
                        mapRet.put(p.getFullPath(), oneColDataList);
                    }

                } catch (ProcessorException e) {
                    throw new IOException(e.getMessage());
                }
                return false;
            }
        };
    }

    /**
     * CrossQuery #3: Function for Cross Columns Query from specific RowGroup(s)
     *
     * @param paths
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @param RowGroupIdxList: IndexList for RowGroup(s) to be read
     * @return QueryDataSet
     * @throws IOException
     */
    public QueryDataSet crossColumnQuery(List<Path> paths, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                         CrossSeriesFilterExpression valueFilter, ArrayList<Integer> RowGroupIdxList) throws IOException {
        CrossQueryTimeGenerator timeQueryDataSet = new CrossQueryTimeGenerator(timeFilter, freqFilter, valueFilter, FETCH_SIZE) {
            @Override
            public DynamicOneColumnData getDataInNextBatch(DynamicOneColumnData res, int fetchSize,
                                                           SingleSeriesFilterExpression valueFilter) throws ProcessorException, IOException {
                return recordReader.getValuesUseFilter(res, fetchSize, valueFilter, RowGroupIdxList);
            }
        };

        return new CrossQueryIteratorDataSet(timeQueryDataSet) {
            @Override
            public boolean getMoreRecords() throws IOException {
                try {
                    long[] timeRet = timeQueryDataSet.generateTimes();
                    if (timeRet.length == 0) {
                        return true;
                    }
                    for (Path p : paths) {
                        String deltaObjectUID = p.getDeltaObjectToString();
                        String measurementUID = p.getMeasurementToString();
                        DynamicOneColumnData oneColDataList = recordReader.getValuesUseTimeValue(deltaObjectUID, measurementUID, timeRet, RowGroupIdxList);
                        mapRet.put(p.getFullPath(), oneColDataList);
                    }

                } catch (ProcessorException e) {
                    throw new IOException(e.getMessage());
                }
                return false;
            }
        };
    }

    public HashMap<String, ArrayList<SeriesSchema>> getAllSeriesSchemasGroupByDeltaObject() throws IOException {
        return recordReader.getAllSeriesSchemasGroupByDeltaObject();
    }

    public HashMap<String, Integer> getDeltaObjectRowGroupCount() throws IOException {
        return recordReader.getDeltaObjectRowGroupCounts();
    }

    public HashMap<String, String> getDeltaObjectTypes() throws IOException {
        return recordReader.getDeltaObjectTypes();
    }

    public TSDataType getSeriesType(Path path) {
        FilterSeries<?> col = recordReader.getColumnByMeasurementName(path.getDeltaObjectToString(), path.getMeasurementToString());
        if (col != null) {
            return col.getSeriesDataType();
        }
        return null;
    }

    public boolean pathExist(Path path) {
        FilterSeries<?> col = recordReader.getColumnByMeasurementName(path.getDeltaObjectToString(), path.getMeasurementToString());

        return col != null;
    }

    public ArrayList<String> getAllDeltaObject() {
        return recordReader.getAllDeltaObjects();
    }

    public ArrayList<SeriesSchema> getAllSeriesSchema() {
        return recordReader.getAllSeriesSchema();
    }

    // Start - Methods for spark reading
    public ArrayList<Long> getRowGroupPosList() {
        return recordReader.getRowGroupPosList();
    }

    public ArrayList<Integer> calSpecificRowGroupByPartition(long start, long end) {
        ArrayList<Long> rowGroupsPosList = getRowGroupPosList();
        ArrayList<Integer> res = new ArrayList<>();
        long curStartPos = 0L;
        for (int i = 0; i < rowGroupsPosList.size(); i++) {
            long curEndPos = rowGroupsPosList.get(i);
            long midPos = curStartPos + (curEndPos - curStartPos) / 2;
            if (start < midPos && midPos <= end) {
                res.add(i);
            }
            curStartPos = curEndPos;
        }
        return res;
    }

    public ArrayList<String> getAllDeltaObjectUIDByPartition(long start, long end) {
        ArrayList<Long> rowGroupsPosList = getRowGroupPosList();
        List<RowGroupReader> rgrs = recordReader.getAllRowGroupReaders();
        ArrayList<String> res = new ArrayList<>();
        long curStartPos = 0L;
        for (int i = 0; i < rowGroupsPosList.size(); i++) {
            long curEndPos = rowGroupsPosList.get(i);
            long midPos = curStartPos + (curEndPos - curStartPos) / 2;
            if (start < midPos && midPos <= end) {
                res.add(rgrs.get(i).getDeltaObjectUID());
            }
            curStartPos = curEndPos;
        }
        return res;
    }

    public Map<String, String> getProps() {
        return recordReader.getProps();
    }

    public String getProp(String key) {
        return recordReader.getProp(key);
    }
}
