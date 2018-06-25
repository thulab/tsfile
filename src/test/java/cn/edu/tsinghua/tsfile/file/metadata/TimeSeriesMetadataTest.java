package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.FreqType;
import cn.edu.tsinghua.tsfile.format.TimeSeries;

public class TimeSeriesMetadataTest {
    public static final String measurementUID = "sensor01";
    public static final int typeLength = 1024;
    final String PATH = "target/TimeSeriesMetaDataBytes.txt";

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {
        File file = new File(PATH);
        if (file.exists())
            file.delete();
    }

    @Test
    public void testWriteIntoFile() throws IOException {
        TimeSeriesChunkMetaData metaData = TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF();
        File file = new File(PATH);
        if (file.exists())
            file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
        ReadWriteThriftFormatUtils.write(metaData.convertToThrift(), out.getOutputStream());

        out.close();
        fos.close();

        FileInputStream fis = new FileInputStream(new File(PATH));
        Utils.isTimeSeriesChunkMetaDataEqual(metaData, metaData.convertToThrift());
        Utils.isTimeSeriesChunkMetaDataEqual(metaData,
                ReadWriteThriftFormatUtils.read(fis, new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData()));
    }
}
