package cn.edu.thu.tsfile.file.metadata.statistics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FloatStatisticsTest {
  private static final float maxError = 0.0001f;

  @Test
  public void testUpdate() {
    Statistics<Float> floatStats = new FloatStatistics();
    floatStats.updateStats(1.34f);
    assertEquals(false, floatStats.isEmpty());
    floatStats.updateStats(2.32f);
    assertEquals(false, floatStats.isEmpty());
    assertEquals(2.32f, (double) floatStats.getMax(), maxError);
    assertEquals(1.34f, (double) floatStats.getMin(), maxError);
  }

  @Test
  public void testMerge() {
    Statistics<Float> floatStats1 = new FloatStatistics();
    Statistics<Float> floatStats2 = new FloatStatistics();

    floatStats1.updateStats(1.34f);
    floatStats1.updateStats(100.13453f);

    floatStats2.updateStats(200.435f);

    Statistics<Float> floatStats3 = new FloatStatistics();
    floatStats3.mergeStatistics(floatStats1);
    assertEquals(false, floatStats3.isEmpty());
    assertEquals(100.13453f, (float) floatStats3.getMax(), maxError);
    assertEquals(1.34f, (float) floatStats3.getMin(), maxError);

    floatStats3.mergeStatistics(floatStats2);
    assertEquals(200.435d, (float) floatStats3.getMax(), maxError);
    assertEquals(1.34d, (float) floatStats3.getMin(), maxError);


  }

}
