package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class TsFileReadPerformanceTest {

    static String filePath = "/Users/beyyes/Desktop/data-0.8/data/settled/root.performf.group_0/1535558400000-1539661667613";

    static int deviceNum = 1;
    static int sensorNum = 2;
    static int SIZE = 10000000;

    public static void main(String args[]) throws IOException, InterruptedException {

        //dynamicWithTsPrimitiveTest();

        //TimeUnit.SECONDS.sleep(10);
        //readTestV7WithoutFilter();
        readTestV8WithoutFilter();

        //readTestV7WithTimeFilter();
        //readTestV8WithTimeFilter();
    }

    private static void dynamicWithTsPrimitiveTest() {
        DynamicOneColumnData dynamicData = new DynamicOneColumnData(TSDataType.INT32, true);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < SIZE; i++) {
            dynamicData.putTime(i);
            dynamicData.putInt(i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("before, consume time : %sms", endTime - startTime));

        startTime = System.currentTimeMillis();
        for (int i = 0; i < SIZE; i++) {
            TimeValuePair tp = new TimeValuePair(i, new TsPrimitiveType.TsInt(i));
        }
        endTime = System.currentTimeMillis();
        System.out.println(String.format("after, consume time : %sms", endTime - startTime));
    }

    private static void readTestV7WithoutFilter() throws IOException {
        // read example : no filter
        TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(filePath);
        TsFile readTsFile = new TsFile(input);
        ArrayList<Path> paths = new ArrayList<>();
        for (int i = 1; i <= deviceNum; i++) {
            for (int j = 0; j < sensorNum; j++) {
                paths.add(new Path(getPerformPath(i, j)));
            }
        }

        long startTime = System.currentTimeMillis();
        OnePassQueryDataSet onePassQueryDataSet = readTsFile.query(paths, null, null);
        int cnt = 0;
        while (onePassQueryDataSet.hasNextRecord()) {
            OldRowRecord record = onePassQueryDataSet.getNextRecord();
//            if (cnt % 5000 == 0) {
                System.out.println(record.toString());
//            }
            cnt++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("consume time : %sms, row count : %s", endTime - startTime, cnt));

        readTsFile.close();
    }

    private static void readTestV8WithoutFilter() throws IOException {
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
        ReadOnlyTsFile tsFile = new ReadOnlyTsFile(randomAccessFileReader);
        QueryExpression queryExpression = QueryExpression.create();
        for (int i = 1; i <= deviceNum; i++) {
            for (int j = 0; j < sensorNum; j++) {
                queryExpression.addSelectedPath(new Path(getPerformPath(i, j)));
            }
        }

        long startTime = System.currentTimeMillis();
        int cnt = 0;
        QueryDataSet queryDataSet = tsFile.query(queryExpression);
        while (queryDataSet.hasNext()) {
            OldRowRecord record = queryDataSet.nextRowRecord();
            System.out.println(record.toString());
//            if (cnt % 5000 == 0) {
//                System.out.println(record.toString());
//            }
            cnt ++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("consume time : %sms, row count : %s", endTime - startTime, cnt));
        tsFile.close();
    }

    private static void readTestV7WithTimeFilter() throws IOException {
        TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(filePath);
        TsFile readTsFile = new TsFile(input);
        ArrayList<Path> paths = new ArrayList<>();
        for (int i = 1; i <= deviceNum; i++) {
            for (int j = 0; j < sensorNum; j++) {
                paths.add(new Path(getPerformPath(i, j)));
            }
        }

        float filterValue = 20.0f;
        FilterExpression valueFilter = FilterFactory.gtEq(FilterFactory.floatFilterSeries(
                "root.performf.group_0.d_0", "s_0", FilterSeriesType.VALUE_FILTER), filterValue, true);
        FilterExpression crossFilter = FilterFactory.csAnd(valueFilter, valueFilter);

        long startTime = System.currentTimeMillis();
        OnePassQueryDataSet onePassQueryDataSet = readTsFile.query(paths, null, crossFilter);
        int cnt = 0;
        while (onePassQueryDataSet.hasNextRecord()) {
            OldRowRecord record = onePassQueryDataSet.getNextRecord();
//            if (cnt == 0 || cnt >= 19999) {
////                System.out.println(record.toString());
////            }
            cnt++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("consume time : %sms, row count : %s", endTime - startTime, cnt));

        readTsFile.close();
    }

    private static void readTestV8WithTimeFilter() throws IOException {
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(filePath);
        ReadOnlyTsFile tsFile = new ReadOnlyTsFile(randomAccessFileReader);
        QueryExpression queryExpression = QueryExpression.create();
        for (int i = 1; i <= deviceNum; i++) {
            for (int j = 0; j < sensorNum; j++) {
                queryExpression.addSelectedPath(new Path(getPerformPath(i, j)));
            }
        }

        float filterValue = 20.0f;
        Filter<Float> valueFilter = ValueFilter.gtEq(filterValue);
        QueryFilter valueQueryFilter = new SeriesFilter(new Path("root.performf.group_0.d_0.s_0"), valueFilter);
        queryExpression.setQueryFilter(valueQueryFilter);

        long startTime = System.currentTimeMillis();
        int cnt = 0;
        QueryDataSet queryDataSet = tsFile.query(queryExpression);
        while (queryDataSet.hasNext()) {
            RowRecord record = queryDataSet.next();
//            if (cnt == 0 || cnt >= 19999) {
//                System.out.println(record.toString());
//            }
            cnt ++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("consume time : %sms, row count : %s", endTime - startTime, cnt));
        tsFile.close();
    }

    private static String getPerformPath(int device, int sensor) {
        return String.format("root.performf.group_0.d_%s.s_%s", device, sensor);
    }
}
