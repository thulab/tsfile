package cn.edu.thu.tsfile.file.metadata.statistics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author CGF
 */
public class BooleanStatisticsTest {
    @Test
    public void testUpdate() {
        Statistics<Boolean> booleanStatistics = new BooleanStatistics();
        booleanStatistics.updateStats(true);
        assertEquals(false, booleanStatistics.isEmpty());
        booleanStatistics.updateStats(false);
        assertEquals(false, booleanStatistics.isEmpty());
        assertEquals(true, (boolean) booleanStatistics.getMax());
        assertEquals(false, (boolean) booleanStatistics.getMin());
    }

    @Test
    public void testMerge() {
        Statistics<Boolean> booleanStats1 = new BooleanStatistics();
        Statistics<Boolean> booleanStats2 = new BooleanStatistics();

        booleanStats1.updateStats(false);
        booleanStats1.updateStats(false);

        booleanStats2.updateStats(true);

        Statistics<Boolean> booleanStats3 = new BooleanStatistics();
        booleanStats3.mergeStatistics(booleanStats1);
        assertEquals(false, booleanStats3.isEmpty());
        assertEquals(false, (boolean) booleanStats3.getMax());
        assertEquals(false, (boolean) booleanStats3.getMin());

        booleanStats3.mergeStatistics(booleanStats2);
        assertEquals(true, (boolean) booleanStats3.getMax());
        assertEquals(false, (boolean) booleanStats3.getMin());
    }
}
