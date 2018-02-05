package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadWriteBytesUtilsTest {

    final String PATH = "target/output1.ksn";
    final int VERSION = 123;

    public static Map<String, String> properties = new HashMap<>();
    static {
        properties.put("s1", "sensor1");
        properties.put("s2", "sensor2");
        properties.put("s3", "sensor3");
    }

    public static Map<String, TsDeltaObject> tsDeltaObjectMap = new HashMap<>();
    static {
        tsDeltaObjectMap.put("d1", new TsDeltaObject(123, 456, 789, 901));
        tsDeltaObjectMap.put("d2", new TsDeltaObject(123, 456, 789, 901));
        tsDeltaObjectMap.put("d3", new TsDeltaObject(123, 456, 789, 901));
    }

    @After
    public void tearDown() throws Exception {
        File file = new File(PATH);
        if (file.exists())
            file.delete();
    }

    @Test
    public void fileWithContentTest() throws IOException {
        int contentLen = 100;
        byte[] content = new byte[contentLen];

        TsFileMetaData tsfMetaData = new TsFileMetaData(tsDeltaObjectMap, null, VERSION);
        tsfMetaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesInTSF());
        tsfMetaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesInTSF());
        tsfMetaData.setCreatedBy("tsf");
        List<String> jsonMetaData = new ArrayList<String>();
        jsonMetaData.add("fsdfsfsd");
        jsonMetaData.add("424fd");
        tsfMetaData.setJsonMetaData(jsonMetaData);

        tsfMetaData.setProps(properties);
        tsfMetaData.addProp("key1", "value1");

        File file = new File(PATH);
        if (file.exists())
            file.delete();
        TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(out.getOutputStream());
        Assert.assertEquals(0, out.getPos());

        out.write(content);
        Assert.assertEquals(contentLen, out.getPos());
        ReadWriteToBytesUtils.write(tsfMetaData, bufferedOutputStream);
        bufferedOutputStream.flush();
        System.out.println(out.getPos());

        bufferedOutputStream.close();
        out.close();

        FileInputStream fis = new FileInputStream(new File(PATH));
        fis.skip(contentLen);
        TsFileMetaData tsfMetaData2 = ReadWriteToBytesUtils.readTsFileMetaData(new BufferedInputStream(fis));
        Utils.isFileMetaDataEqual(tsfMetaData, tsfMetaData2);
    }
}
