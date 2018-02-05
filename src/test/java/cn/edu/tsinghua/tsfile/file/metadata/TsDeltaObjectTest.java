package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TsDeltaObjectTest {

    final String PATH = "target/outputDeltaObject.ksn";

    @Test
    public void testWriteIntoFileByBytes() throws IOException {
        TsDeltaObject metaData = new TsDeltaObject(11111L, 22222, 33333L, 44444L);
        File file = new File(PATH);
        if (file.exists())
            file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
        ReadWriteToBytesUtils.write(metaData, out.getOutputStream());

        out.close();
        fos.close();

        FileInputStream fis = new FileInputStream(new File(PATH));
        TsDeltaObject metaData2 = ReadWriteToBytesUtils.readTsDeltaObject(fis);
        fis.close();
        Utils.isDeltaObjectEqual(metaData, metaData2);
    }
}
