package cn.edu.tsinghua.tsfile.timeseries.utils;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.And;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.Lt;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryPerfTest {

    private String SENSOR_PREFIX = "s";
    private String DEVICE_PREFIX = "d";
    private String SEPARATOR = ".";

    private String filePath = "gen.ts_plain";
    private int ptNum = 1000000;

    private int selectNum = 1;
    int conditionPathNum = 1;
    double selectRate = 0.1;

    private QueryExecutorRouter router;

    public void initEngine() throws IOException {
        TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        reader.open();
        router = new QueryExecutorRouter(new MetadataQuerierByFileImpl(reader),
                                         new SeriesChunkLoaderImpl(reader));
    }

    public void testQuery() throws IOException {
        initEngine();

        List<Path> selectPaths = new ArrayList<>();
        for (int i = 0; i < selectNum; i++) {
            selectPaths.add(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + i));
        }

        QueryExpression expression = QueryExpression.create();
        expression.setSelectSeries(selectPaths);
        long startTime;

        // condition paths all in select paths
        List<QueryFilter> seriesFilters = new ArrayList<>();
        for (int i = 0; i < conditionPathNum; i++) {
            seriesFilters.add(new SeriesFilter<Double>(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + i),
                    ValueFilter.lt(selectRate * ptNum)));
        }
        QueryFilter finalFilter = null;
        for (QueryFilter filter : seriesFilters) {
            if (finalFilter == null) {
                finalFilter = filter;
            } else {
                finalFilter = QueryFilterFactory.and(finalFilter, filter);
            }
        }
        expression.setQueryFilter(finalFilter);
        startTime = System.currentTimeMillis();
        QueryDataSet dataSet = router.execute(expression);
        int cnt = 0;
        while (dataSet.hasNext()) {
            dataSet.next();
            cnt ++;
        }
        System.out.println(String.format("******Query with Inner Conditions******: %d ms, %d pts", (System.currentTimeMillis() - startTime), cnt));

        // condition paths partially in select paths
        seriesFilters.clear();
        for (int i = 0; i < (int) (0.5 * conditionPathNum); i++) {
            seriesFilters.add(new SeriesFilter<Double>(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + i),
                    new Lt<Double>(selectRate * ptNum, FilterType.VALUE_FILTER)));
        }
        for (int i = (int) (0.5 * conditionPathNum); i < conditionPathNum; i++) {
            seriesFilters.add(new SeriesFilter<Double>(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + (i + 1)),
                    new Lt<Double>(selectRate * ptNum, FilterType.VALUE_FILTER)));
        }
        finalFilter = null;
        for (QueryFilter filter : seriesFilters) {
            if (finalFilter == null) {
                finalFilter = filter;
            } else {
                finalFilter = QueryFilterFactory.and(finalFilter, filter);
            }
        }
        expression.setQueryFilter(finalFilter);
        startTime = System.currentTimeMillis();
        dataSet = router.execute(expression);
        cnt = 0;
        while (dataSet.hasNext()) {
            dataSet.next();
            cnt ++;
        }
        System.out.println(String.format("******Query with cross Conditions******: %d ms, %d pts", (System.currentTimeMillis() - startTime), cnt));

        // condition paths outside select paths
        seriesFilters.clear();
        for (int i = 0; i < conditionPathNum; i++) {
            seriesFilters.add(new SeriesFilter<Double>(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + (i + 1)),
                    new Lt<Double>(selectRate * ptNum, FilterType.VALUE_FILTER)));
        }
        finalFilter = null;
        for (QueryFilter filter : seriesFilters) {
            if (finalFilter == null) {
                finalFilter = filter;
            } else {
                finalFilter = QueryFilterFactory.and(finalFilter, filter);
            }
        }
        expression.setQueryFilter(finalFilter);
        startTime = System.currentTimeMillis();
        dataSet = router.execute(expression);
        cnt = 0;
        while (dataSet.hasNext()) {
            dataSet.next();
            cnt ++;
        }
        System.out.println(String.format("******Query with outer Conditions******: %d ms, %d pts", (System.currentTimeMillis() - startTime), cnt));

        // time condition only
        finalFilter = new GlobalTimeFilter(TimeFilter.lt((long) (selectRate * ptNum)));
        expression.setQueryFilter(finalFilter);
        startTime = System.currentTimeMillis();
        dataSet = router.execute(expression);
        cnt = 0;
        while (dataSet.hasNext()) {
            dataSet.next();
            cnt ++;
        }
        System.out.println(String.format("******Query with Time Condition******: %d ms, %d pts", (System.currentTimeMillis() - startTime), cnt));

        // no condition
        expression = QueryExpression.create();
        expression.setSelectSeries(selectPaths);
        startTime = System.currentTimeMillis();
        dataSet = router.execute(expression);
        cnt = 0;
        while (dataSet.hasNext()) {
            dataSet.next();
            cnt ++;
        }
        System.out.println(String.format("******Query without Conditions******: %d ms, %d pts", (System.currentTimeMillis() - startTime), cnt));
    }

    public static void main(String[] args) throws IOException {
        QueryPerfTest test = new QueryPerfTest();
        test.testQuery();
    }
}