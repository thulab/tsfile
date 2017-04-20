package cn.edu.thu.tsfile.timeseries.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.SingleValueVisitorFactory;
import org.junit.Test;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.utils.freq.FrequencyUtil;

/**
 * Created by kangrong on 16/11/24.
 */
public class FrequencyUtilTest {
    SingleValueVisitor<?> floatVisitor = SingleValueVisitorFactory.getSingleValueVistor(TSDataType.FLOAT);

    @Test
    public void testFreq() {
        LtEq<Float> ltEq = FilterFactory.ltEq(FilterFactory.floatFilterSeries("a", "b", FilterSeriesType.FREQUENCY_FILTER),4.5f,true);
        assertFalse(floatVisitor.satisfyObject(10f, ltEq));
        assertTrue(floatVisitor.satisfyObject(4.5f, ltEq));
        assertTrue(floatVisitor.satisfyObject(4.4f, ltEq));
        List<float[]> input = new ArrayList<>();
        input.add(new float[]{0.0f,4.5f});
        assertTrue(FrequencyUtil.satisfy(ltEq,input));
        for (int i = 0; i < 8; i++) {
            input.add(new float[]{0.0f,4.6f});
            assertTrue(FrequencyUtil.satisfy(ltEq,input));
        }
        input.add(new float[]{0.0f,4.6f});
        assertFalse(FrequencyUtil.satisfy(ltEq,input));

    }
}
