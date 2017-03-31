package com.corp.delta.tsfile.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.corp.delta.tsfile.filter.definition.FilterFactory;
import com.corp.delta.tsfile.filter.definition.filterseries.FilterSeriesType;
import com.corp.delta.tsfile.filter.definition.operators.And;
import com.corp.delta.tsfile.filter.definition.operators.Eq;
import com.corp.delta.tsfile.filter.definition.operators.GtEq;
import com.corp.delta.tsfile.filter.definition.operators.LtEq;
import com.corp.delta.tsfile.filter.definition.operators.Not;
import com.corp.delta.tsfile.filter.definition.operators.NotEq;
import com.corp.delta.tsfile.filter.definition.operators.Or;
import com.corp.delta.tsfile.filter.visitorImpl.OverflowTimeFilter;

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
