package cn.edu.tsinghua.tsfile.timeseries.read.query.executor;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV1.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.impl.QueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.query.impl.QueryWithQueryFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.query.impl.QueryWithoutFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class QueryExecutorTest {


    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private TsFileSequenceReader fileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private SeriesChunkLoader seriesChunkLoader;
    private int rowCount = 10000;
    private QueryWithQueryFilterExecutorImpl queryExecutorWithQueryFilter;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
        fileReader = new TsFileSequenceReader(FILE_PATH);
        metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
        seriesChunkLoader = new SeriesChunkLoaderImpl(fileReader);
        queryExecutorWithQueryFilter = new QueryWithQueryFilterExecutorImpl(seriesChunkLoader, metadataQuerierByFile);
    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void query1() throws IOException {
        Filter<Integer> filter = TimeFilter.lt(1480562618100L);
        Filter<Binary> filter2 = ValueFilter.gt(new Binary("dog"));

        QueryFilter queryFilter = QueryFilterFactory.and(
                new SeriesFilter<>(new Path("d1.s1"), filter),
                new SeriesFilter<>(new Path("d1.s4"), filter2)
        );

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"))
                .setQueryFilter(queryFilter);
        long startTimestamp = System.currentTimeMillis();
        QueryDataSet queryDataSet = queryExecutorWithQueryFilter.execute(queryExpression);
        long aimedTimestamp = 1480562618000L;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            System.out.println(rowRecord);
            aimedTimestamp += 8;
        }
        long endTimestamp = System.currentTimeMillis();
        System.out.println("[Query]:" + queryExpression + "\n[Time]: " + (endTimestamp - startTimestamp) + "ms");
    }

    @Test
    public void queryWithoutFilter() throws IOException {
        QueryExecutor queryExecutor = new QueryWithoutFilterExecutorImpl(seriesChunkLoader, metadataQuerierByFile);

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"));

        long aimedTimestamp = 1480562618000L;
        int count = 0;
        long startTimestamp = System.currentTimeMillis();
        QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            aimedTimestamp++;
            count++;
        }
        Assert.assertEquals(rowCount, count);
        long endTimestamp = System.currentTimeMillis();
        System.out.println("[Query]:" + queryExpression + "\n[Time]: " + (endTimestamp - startTimestamp) + "ms");
    }

    @Test
    public void queryWithGlobalTimeFilter() throws IOException {
        QueryExecutor queryExecutor = new QueryWithGlobalTimeFilterExecutorImpl(seriesChunkLoader, metadataQuerierByFile);

        QueryFilter queryFilter = new GlobalTimeFilter(FilterFactory.and(TimeFilter.gtEq(1480562618100L), TimeFilter.lt(1480562618200L)));
        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"))
                .setQueryFilter(queryFilter);


        long aimedTimestamp = 1480562618100L;
        int count = 0;
        long startTimestamp = System.currentTimeMillis();
        QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            aimedTimestamp++;
            count++;
        }
        Assert.assertEquals(100, count);
        long endTimestamp = System.currentTimeMillis();
        System.out.println("[Query]:" + queryExpression + "\n[Time]: " + (endTimestamp - startTimestamp) + "ms");
    }
}
