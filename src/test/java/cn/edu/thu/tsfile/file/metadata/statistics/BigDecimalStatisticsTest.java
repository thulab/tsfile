package cn.edu.thu.tsfile.file.metadata.statistics;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.junit.Test;

public class BigDecimalStatisticsTest {
  
  @Test
  public void testUpdate() {
    Statistics<BigDecimal> bigStats = new BigDecimalStatistics();
    BigDecimal up1 = new BigDecimal("1.232");
    BigDecimal up2 = new BigDecimal("2.232");
    bigStats.updateStats(up1);
    assertEquals(false, bigStats.isEmpty());
    bigStats.updateStats(up2);
    assertEquals(false, bigStats.isEmpty());
    assertEquals(up2, (BigDecimal) bigStats.getMax());
    assertEquals(up1, (BigDecimal) bigStats.getMin());
  }

  @Test
  public void testMerge() {
    Statistics<BigDecimal> bigStats1 = new BigDecimalStatistics();
    Statistics<BigDecimal> bigStats2 = new BigDecimalStatistics();

    BigDecimal down1 = new BigDecimal("1.232");
    BigDecimal up1 = new BigDecimal("2.232");
    bigStats1.updateStats(down1);
    bigStats1.updateStats(up1);
    BigDecimal up2 = new BigDecimal("200.232");
    bigStats2.updateStats(up2);

    Statistics<BigDecimal> bigStats3 = new BigDecimalStatistics();
    bigStats3.mergeStatistics(bigStats1);
    assertEquals(false, bigStats3.isEmpty());
    assertEquals(up1, (BigDecimal) bigStats3.getMax());
    assertEquals(down1, (BigDecimal) bigStats3.getMin());

    bigStats3.mergeStatistics(bigStats2);
    assertEquals(up2, (BigDecimal) bigStats3.getMax());
    assertEquals(down1, (BigDecimal) bigStats3.getMin());


  }

}
