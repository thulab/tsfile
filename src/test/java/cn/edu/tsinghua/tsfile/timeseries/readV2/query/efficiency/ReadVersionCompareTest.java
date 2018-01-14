package cn.edu.tsinghua.tsfile.timeseries.readV2.query.efficiency;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.timeseries.readV2.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryWithQueryFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/2.
 */
@Ignore
public class ReadVersionCompareTest {

    private static final String filePath = "/users/zhangjinrui/Desktop/testTsFile.ts";

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {

    }

    @After
    public void after() throws IOException {
//        TsFileGeneratorForTest.after();
    }

    @Test
    public void oldVersion() {

    }

    @Test
    public void newVersion() throws IOException {

        QueryFilter queryFilter = new SeriesFilter<>(new Path("d1.s1"), FilterFactory.and(
                ValueFilter.gt(0), ValueFilter.lt(100)));

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .setQueryFilter(queryFilter);

        long startTime = System.currentTimeMillis();
        int count = 0;
        ReadOnlyTsFile tsFile = new ReadOnlyTsFile(new TsRandomAccessLocalFileReader(filePath));
        QueryDataSet queryDataSet = tsFile.query(queryExpression);
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            System.out.println(rowRecord);
            count++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Read count: " + count + ". Time used :" + (endTime - startTime) + "ms");

        tsFile.close();
    }
}
