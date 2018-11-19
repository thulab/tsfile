package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TimeSeriesMetadataTest {
    public static final String measurementUID = "sensor01";
    public static final int typeLength = 1024;
    final String PATH = "target/outputTimeSeries.tsfile";

    @Before
    public void setUp() {}

    @After
    public void tearDown() {
        File file = new File(PATH);
        if (file.exists())
            file.delete();
    }

    @Test
    public void testWriteIntoFile() throws IOException {
        MeasurementSchema measurementSchema = TestHelper.createSimpleMeasurementSchema();
        File file = new File(PATH);
        if (file.exists())
            file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        measurementSchema.serializeTo(fos);
        fos.close();

        FileInputStream fis = new FileInputStream(new File(PATH));
        measurementSchema.equals(MeasurementSchema.deserializeFrom(fis));
    }
}
