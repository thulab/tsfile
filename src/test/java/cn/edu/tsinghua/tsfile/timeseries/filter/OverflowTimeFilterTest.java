package cn.edu.tsinghua.tsfile.timeseries.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.OverflowTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Or;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.Eq;

/**
 * 
 * @author CGF
 *
 */
public class OverflowTimeFilterTest {
    
    private static final OverflowTimeFilter filter = new OverflowTimeFilter();
	private static String deltaObjectUID = FilterTestConstant.deltaObjectUID;
	private static String measurementUID = FilterTestConstant.measurementUID;
	
    @Test
    public void test() {
        
        Eq<Long> eq = FilterFactory.eq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45L);
        NotEq<Long> noteq = FilterFactory.noteq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45L);
        LtEq<Long> lteq = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45L, true);
        GtEq<Long> gteq = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 45L, true);
        LtEq<Long> left = FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 55L, true);
        GtEq<Long> right = FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObjectUID, measurementUID, FilterSeriesType.VALUE_FILTER), 35L, true);
        And and = (And) FilterFactory.and(left, right);
        Or or = (Or) FilterFactory.or(left, right);
        Not not = (Not) FilterFactory.not(and);
        
        assertTrue(filter.satisfy(eq, 10L, 50L));
        
        assertTrue(filter.satisfy(noteq, 10L, 30L));
        assertFalse(filter.satisfy(noteq, 45L, 45L));
        
        assertTrue(filter.satisfy(lteq, 10L, 50L));
        
        assertTrue(filter.satisfy(gteq, 10L, 50L));
        
        assertTrue(filter.satisfy(and, 10L, 50L));
        
        assertTrue(filter.satisfy(or, 10L, 50L));
        
        assertFalse(filter.satisfy(not, 10L, 50L));
    }
}
