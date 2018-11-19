package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV1.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class TimestampGeneratorTest {

    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private TsFileSequenceReader fileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private SeriesChunkLoader seriesChunkLoader;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForTest.generateFile(1000, 10 * 1024 * 1024, 10000);
        fileReader = new TsFileSequenceReader(FILE_PATH);
        metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
        seriesChunkLoader = new SeriesChunkLoaderImpl(fileReader);
    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void testTimeGenerator() throws IOException {
        long startTimestamp = 1480562618000L;
        Filter<Integer> filter = TimeFilter.lt(1480562618100L);
        Filter<Binary> filter2 = ValueFilter.gt(new Binary("dog"));
        Filter<Integer> filter3 = FilterFactory.and(TimeFilter.gtEq(1480562618000L), TimeFilter.ltEq(1480562618100L));

        QueryFilter queryFilter = QueryFilterFactory.or(
                QueryFilterFactory.and(
                        new SeriesFilter<>(new Path("d1.s1"), filter),
                        new SeriesFilter<>(new Path("d1.s4"), filter2)
                ),
                new SeriesFilter<>(new Path("d1.s1"), filter3));

        TimestampGeneratorByQueryFilterImpl timestampGenerator = new TimestampGeneratorByQueryFilterImpl(queryFilter, seriesChunkLoader, metadataQuerierByFile);
        while (timestampGenerator.hasNext()) {
//            System.out.println(timestampGenerator.next());
            Assert.assertEquals(startTimestamp, timestampGenerator.next());
            startTimestamp += 1;
        }
        Assert.assertEquals(1480562618101L, startTimestamp);
    }
}
