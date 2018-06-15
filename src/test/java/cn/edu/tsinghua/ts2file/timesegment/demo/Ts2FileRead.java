package cn.edu.tsinghua.ts2file.timesegment.demo;

import cn.edu.tsinghua.ts2file.timesegment.basis.Ts2File;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import java.util.ArrayList;

/**
 * Created by qmm on 18/3/18.
 */
public class Ts2FileRead {

  public static void main(String[] args) throws Exception {
    String path = "test.tw";
    long start = System.currentTimeMillis();
    // read example : no filter
    TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(path);
    Ts2File readTs2File = new Ts2File(input);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("root.laptop.d1.sum(s1)"));
    FilterExpression timeFilter = FilterFactory.and(FilterFactory.gtEq(FilterFactory.timeFilterSeries(), 1000L, true),
        FilterFactory.ltEq(FilterFactory.timeFilterSeries(), 1000000L, false));
    QueryDataSet queryDataSet = readTs2File.query(paths, timeFilter, null);
    System.out.println(queryDataSet.getClass());
    while (queryDataSet.hasNextRecord()) {
      queryDataSet.getNextRecord();
    }
    System.out.println("------------");
    long end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}
