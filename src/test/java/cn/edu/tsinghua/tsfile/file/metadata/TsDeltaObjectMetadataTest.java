package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TsDeltaObjectMetadataTest {

    public static final long OFFSET = 2313424242L;
    public static final int METADATA_BLOCK_SIZE = 432453453;
    public static final long START_TIME = 523372036854775806L;
    public static final long END_TIME = 523372036854775806L;
    final String PATH = "target/outputDeltaObject.ksn";

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
        TsDeltaObjectMetadata metaData = TestHelper.createSimpleDeltaObjectMetaData();
        File file = new File(PATH);
        if (file.exists())
            file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
        ReadWriteIOUtils.write(metaData, out.getOutputStream());
        out.close();
        fos.close();

        FileInputStream fis = new FileInputStream(new File(PATH));
        Utils.isDeltaObjectEqual(metaData, TsDeltaObjectMetadata.deserializeFrom(fis));
    }
}
