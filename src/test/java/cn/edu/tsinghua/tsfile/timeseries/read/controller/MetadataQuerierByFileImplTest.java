package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.readV1.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
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
public class MetadataQuerierByFileImplTest {

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
        MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
        List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile.getSeriesChunkMetaDataList(new Path("d2.s1"));
        for(ChunkMetaData chunkMetaData: chunkMetaDataList) {
            Assert.assertEquals(chunkMetaData.getMeasurementUID(), "s1");
        }
    }
}
