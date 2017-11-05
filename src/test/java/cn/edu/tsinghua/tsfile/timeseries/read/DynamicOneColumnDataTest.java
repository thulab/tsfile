package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
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

    @Test
    public void emptyTimeTest() {
        DynamicOneColumnData data1 = new DynamicOneColumnData(TSDataType.INT32, true, true);
        for (int i = 1;i <= 10;i ++) {
            if (i % 2 == 0) {
                data1.putTime(i);
                data1.putInt(i);
            } else {
                data1.putEmptyTime(i);
            }
        }

        for (int i = 0;i < data1.valueLength;i++) {
            Assert.assertEquals((i+1)*2, data1.getTime(i));
            Assert.assertEquals((i+1)*2, data1.getInt(i));
        }

        for (int i = 0;i < data1.emptyTimeLength;i++) {
            Assert.assertEquals((i+1)*2, data1.getTime(i));
        }

        DynamicOneColumnData data2 = new DynamicOneColumnData(TSDataType.INT32, true, false);
        for (int i = 5;i <= 20;i ++) {
            data2.putTime(i);
            data1.putInt(i);
        }
    }
}
