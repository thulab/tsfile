package cn.edu.tsinghua.tsfile.timeseries.readV2;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesChunkReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesChunkReaderByTimestampImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesChunkReaderWithFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesChunkReaderWithoutFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.series.ISeriesWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.SeriesWriterImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by zhangjinrui on 2017/12/24.
 */
public class SeriesChunkReaderTest {

    private static final int PAGE_SIZE_THRESHOLD = 1000000;
    private static final int MAX_PAGE_VALUE_COUNT = 1000;
    private static final int TIME_VALUE_PAIR_COUNT = 1000000;
    private ISeriesWriter seriesWriter;
    private List<Object> ret;

    private boolean duplicateIncompletedPage;
    private int maxNumberOfPointsInPage;
    private String timeSeriesEncoder;

    @Before
    public void before() {
        duplicateIncompletedPage = TSFileDescriptor.getInstance().getConfig().duplicateIncompletedPage;
        maxNumberOfPointsInPage = TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage;
        timeSeriesEncoder = TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder;
        TSFileDescriptor.getInstance().getConfig().duplicateIncompletedPage = true;
        TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage = MAX_PAGE_VALUE_COUNT;
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
    }

    @After
    public void after() {
        TSFileDescriptor.getInstance().getConfig().duplicateIncompletedPage = duplicateIncompletedPage;
        TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage = maxNumberOfPointsInPage;
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = timeSeriesEncoder;
    }

    private ByteArrayInputStream getSeriesChunk() {
        MeasurementDescriptor measurementDescriptor = new MeasurementDescriptor("s1", TSDataType.INT64, TSEncoding.RLE);
        IPageWriter pageWriter = new PageWriterImpl(measurementDescriptor);
        seriesWriter = new SeriesWriterImpl("d1", measurementDescriptor, pageWriter, PAGE_SIZE_THRESHOLD);
        try {
            for (int i = 0; i < TIME_VALUE_PAIR_COUNT; i++) {
                seriesWriter.write((long) i, (long) i);
            }
            ret = seriesWriter.query();
            Pair<List<ByteArrayInputStream>, CompressionTypeName> pagePairData = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) ret.get(1);
            List<ByteArrayInputStream> pages = pagePairData.left;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (ByteArrayInputStream page : pages) {
                byte[] buf = new byte[page.available()];
                page.read(buf, 0, buf.length);
                out.write(buf, 0, buf.length);
            }
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        return null;
    }

    private CompressionTypeName getCompressionTypeName() {
        Pair<List<ByteArrayInputStream>, CompressionTypeName> pagePairData = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) ret.get(1);
        return pagePairData.right;
    }

    @Test
    public void readOneSeriesChunkWithoutFilter() {
        try {
            ByteArrayInputStream seriesChunkInputStream = getSeriesChunk();
            SeriesChunkReader seriesChunkReader = new SeriesChunkReaderWithoutFilterImpl(seriesChunkInputStream, TSDataType.INT64, getCompressionTypeName());

            int count = 0;
            long aimedValue = 0;
            long startTimestamp = System.currentTimeMillis();
            while (seriesChunkReader.hasNext()) {
                TimeValuePair timeValuePair = seriesChunkReader.next();
                assertEquals(aimedValue, timeValuePair.getTimestamp());
                assertEquals(aimedValue, timeValuePair.getValue().getLong());
                aimedValue++;
                count++;
            }
            long endTimestamp = System.currentTimeMillis();
            System.out.println("[Read without Filter] Time used: " + (endTimestamp - startTimestamp) + "ms. Count = " + count);

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void readOneSeriesChunkWithFilter() {
        try {
            long valueLeft = 5L;
            long valueRight = 10L;
            long timeLeft = 9000L;
            long timeRight = 9002L;
            Filter<Long> filter = FilterFactory.or(
                    FilterFactory.and(TimeFilter.gtEq(timeLeft), TimeFilter.ltEq(timeRight)),
                    FilterFactory.and(ValueFilter.gtEq(valueLeft), ValueFilter.ltEq(valueRight))
            );
            SeriesFilter<Long> seriesFilter = new SeriesFilter(new Path("d1.s1"), filter);
            ByteArrayInputStream seriesChunkInputStream = getSeriesChunk();
            SeriesChunkReader seriesChunkReader = new SeriesChunkReaderWithFilterImpl(seriesChunkInputStream, TSDataType.INT64,
                    getCompressionTypeName(), seriesFilter.getFilter());
            long aimedValue = valueLeft;
            long startTimestamp = System.currentTimeMillis();
            while (seriesChunkReader.hasNext()) {
                TimeValuePair timeValuePair = seriesChunkReader.next();
                assertEquals(aimedValue, timeValuePair.getTimestamp());
                assertEquals(aimedValue, timeValuePair.getValue().getLong());
                if (aimedValue == valueRight) {
                    aimedValue = timeLeft;
                } else {
                    aimedValue++;
                }
            }
            long endTimestamp = System.currentTimeMillis();
            System.out.println("[Read with Filter] Time used: " + (endTimestamp - startTimestamp) + "ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
