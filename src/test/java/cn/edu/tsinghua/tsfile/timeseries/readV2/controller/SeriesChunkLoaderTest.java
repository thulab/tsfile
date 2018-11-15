package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;


import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.MemSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */

public class SeriesChunkLoaderTest {

    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private TsFileSequenceReader fileReader;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TsFileGeneratorForTest.generateFile(1000000, 1024 * 1024, 10000);
    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void test() throws IOException {
        fileReader = new TsFileSequenceReader(FILE_PATH);
        fileReader.open();
        MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = metadataQuerierByFile.getSeriesChunkDescriptorList(new Path("d2.s1"));

        SeriesChunkLoaderImpl seriesChunkLoader = new SeriesChunkLoaderImpl(fileReader);
        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
            MemSeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(timeSeriesChunkMetaData);
            ChunkHeader chunkHeader = ChunkHeader.deserializeFrom(memSeriesChunk.getSeriesChunkBodyStream());
            Assert.assertEquals(chunkHeader.getDataSize(), memSeriesChunk.getSeriesChunkBodyStream().remaining());
        }
    }
}
