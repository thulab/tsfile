package cn.edu.tsinghua.tsfile.timeseries.readV2.query.efficiency;

import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/2.
 */
@Ignore
public class OldVersionQueryTest {

    private static final String filePath = "/users/zhangjinrui/Desktop/testTsFile.ts";

    @Test
    public void query() throws IOException {
        List<Path> paths = new ArrayList<>();
        paths.add(new Path("d1.s1"));

        FilterExpression valueFilter = FilterFactory.and(
                FilterFactory.gtEq(FilterFactory.intFilterSeries("d1", "s1", FilterSeriesType.VALUE_FILTER),
                        0, false),
                FilterFactory.ltEq(FilterFactory.intFilterSeries("d1", "s1", FilterSeriesType.VALUE_FILTER),
                        100000, false)
        );

        long startTime = System.currentTimeMillis();
        int count = 0;
        TsFile tsFile = new TsFile(new TsRandomAccessLocalFileReader(filePath));
        QueryDataSet queryDataSet = tsFile.query(paths, null, valueFilter);
        while (queryDataSet.hasNextRecord()) {
            RowRecord record = queryDataSet.getNextRecord();
            count++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Read count: " + count + ". Time used :" + (endTime - startTime) + "ms");
    }
}
