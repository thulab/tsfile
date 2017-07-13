package cn.edu.thu.tsfile.file.metadata.statistics;


import cn.edu.thu.tsfile.common.utils.Binary;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringStatisticsTest {
    @Test
    public void testUpdate() {
        Statistics<Binary> binaryStats = new BinaryStatistics();
        binaryStats.updateStats(new Binary("aaa"));
        assertEquals(false, binaryStats.isEmpty());
        binaryStats.updateStats(new Binary("bbb"));
        assertEquals(false, binaryStats.isEmpty());
        assertEquals("bbb", binaryStats.getMax().getStringValue());
        assertEquals("aaa", binaryStats.getMin().getStringValue());
    }

    @Test
    public void testMerge() {
        Statistics<Binary> stringStats1 = new BinaryStatistics();
        Statistics<Binary> stringStats2 = new BinaryStatistics();

        stringStats1.updateStats(new Binary("aaa"));
        stringStats1.updateStats(new Binary("ccc"));

        stringStats2.updateStats(new Binary("ddd"));

        Statistics<Binary> stringStats3 = new BinaryStatistics();
        stringStats3.mergeStatistics(stringStats1);
        assertEquals(false, stringStats3.isEmpty());
        assertEquals("ccc", (String) stringStats3.getMax().getStringValue());
        assertEquals("aaa", (String) stringStats3.getMin().getStringValue());

        stringStats3.mergeStatistics(stringStats2);
        assertEquals("ddd", (String) stringStats3.getMax().getStringValue());
        assertEquals("aaa", (String) stringStats3.getMin().getStringValue());
    }
}
