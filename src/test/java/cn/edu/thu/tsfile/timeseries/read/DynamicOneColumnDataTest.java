package cn.edu.thu.tsfile.timeseries.read;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.junit.Assert;
import org.junit.Test;

/**
 * test the usage of DynamicOneColumnData
 */
public class DynamicOneColumnDataTest {

    private static final int MAXN = 100005;

    @Test
    public void testPutGetMethod() {
        DynamicOneColumnData data = new DynamicOneColumnData(TSDataType.INT32, true);
        for (int i = 0; i < MAXN; i++) {
            data.putTime(i + 10);
        }
        // Assert.assertEquals(data.timeArrayIdx, 100);

        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(data.getTime(i), i + 10);
        }
    }
}
