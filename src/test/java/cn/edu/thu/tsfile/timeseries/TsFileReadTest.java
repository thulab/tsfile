package cn.edu.thu.tsfile.timeseries;

import cn.edu.thu.tsfile.timeseries.FileFormat.TsFile;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;

import java.io.IOException;
import java.util.ArrayList;

public class TsFileReadTest {

    public static void main(String args[]) throws IOException, WriteProcessException {
        String path = "tsfile-impl-parent/tsfile-timeseries/src/test/resources/test.ts";

        // read example : no filter
        LocalFileInput input = new LocalFileInput(path);
        TsFile readTsFile = new TsFile(input);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        paths.add(new Path("device_1.sensor_2"));
        paths.add(new Path("device_1.sensor_3"));
        QueryDataSet queryDataSet = readTsFile.query(paths, null, null);
        while(queryDataSet.hasNextRecord()){
            System.out.println(queryDataSet.getNextRecord());
        }
        System.out.println("------------");

        // time filter : 4 <= time < 10
        FilterExpression timeFilter = FilterFactory.and(FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 4L, true)
                , FilterFactory.ltEq(FilterFactory.timeFilterSeries(), 10L, false));
        input = new LocalFileInput(path);
        readTsFile = new TsFile(input);
        paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        paths.add(new Path("device_1.sensor_2"));
        paths.add(new Path("device_1.sensor_3"));
        queryDataSet = readTsFile.query(paths, timeFilter, null);
        while(queryDataSet.hasNextRecord()){
            System.out.println(queryDataSet.getNextRecord());
        }
        System.out.println("------------");

        // value filter : device_1.sensor_2 < 20
        FilterExpression valueFilter = FilterFactory.ltEq(FilterFactory.intFilterSeries("device_1","sensor_2", FilterSeriesType.VALUE_FILTER), 20, false);
        input = new LocalFileInput(path);
        readTsFile = new TsFile(input);
        paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        paths.add(new Path("device_1.sensor_2"));
        paths.add(new Path("device_1.sensor_3"));
        queryDataSet = readTsFile.query(paths, null, valueFilter);
        while(queryDataSet.hasNextRecord()){
            System.out.println(queryDataSet.getNextRecord());
        }
        System.out.println("------------");

        // time filter : 4 <= time < 10, value filter : device_1.sensor_2 > 20
        timeFilter = FilterFactory.and(FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 4L, true), FilterFactory.ltEq(FilterFactory.timeFilterSeries(), 10L, false));
        valueFilter = FilterFactory.gtEq(FilterFactory.intFilterSeries("device_1","sensor_3", FilterSeriesType.VALUE_FILTER), 21, true);
        input = new LocalFileInput(path);
        readTsFile = new TsFile(input);
        paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        paths.add(new Path("device_1.sensor_2"));
        paths.add(new Path("device_1.sensor_3"));
        queryDataSet = readTsFile.query(paths, timeFilter, valueFilter);
        while(queryDataSet.hasNextRecord()){
            System.out.println(queryDataSet.getNextRecord());
        }
    }
}
