package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DoubleStatisticsTest {
  private static final double maxError = 0.0001d;

  @Test
  public void testUpdate() {
    Statistics<Double> doubleStats = new DoubleStatistics();
    doubleStats.updateStats(1.34d);
    assertEquals(false, doubleStats.isEmpty());
    doubleStats.updateStats(2.32d);
    assertEquals(false, doubleStats.isEmpty());
    assertEquals(2.32d, (double) doubleStats.getMax(), maxError);
    assertEquals(1.34d, (double) doubleStats.getMin(), maxError);
  }

  @Test
  public void testMerge() {
    Statistics<Double> doubleStats1 = new DoubleStatistics();
    Statistics<Double> doubleStats2 = new DoubleStatistics();

    doubleStats1.updateStats(1.34d);
    doubleStats1.updateStats(100.13453d);

    doubleStats2.updateStats(200.435d);

    Statistics<Double> doubleStats3 = new DoubleStatistics();
    doubleStats3.mergeStatistics(doubleStats1);
    assertEquals(false, doubleStats3.isEmpty());
    assertEquals(100.13453d, (double) doubleStats3.getMax(), maxError);
    assertEquals(1.34d, (double) doubleStats3.getMin(), maxError);

    doubleStats3.mergeStatistics(doubleStats2);
    assertEquals(200.435d, (double) doubleStats3.getMax(), maxError);
    assertEquals(1.34d, (double) doubleStats3.getMin(), maxError);


  }

}
