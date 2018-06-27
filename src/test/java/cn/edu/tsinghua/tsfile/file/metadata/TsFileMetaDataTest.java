package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.format.TimeSeries;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.format.DeltaObject;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileMetaDataTest {
    final String PATH = "target/output1.ksn";
    public static final int VERSION = 123;
    public static final String CREATED_BY = "tsf";
    public static final long FIRST_TSMETADATA_OFFSET = 111111111L;
    public static final long LAST_TSMETADATA_OFFSET = 222222222L;
    public static final long FIRST_DOMETADATA_OFFSET = 333333333L;
    public static final long LAST_DOMETADATA_OFFSET = 444444444L;

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    }

    @Test
    public void testWriteFileMetaData() throws IOException {
    TsFileMetaData tsfMetaData = TestHelper.createSimpleFileMetaData();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteToBytesUtils.write(tsfMetaData, out.getOutputStream());
    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isFileMetaDataEqual(tsfMetaData, ReadWriteToBytesUtils.readTsFileMetaData(fis));
    }
}
