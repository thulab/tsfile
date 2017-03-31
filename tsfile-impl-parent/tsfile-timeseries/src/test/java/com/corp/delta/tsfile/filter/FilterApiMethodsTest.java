package com.corp.delta.tsfile.filter;



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corp.delta.tsfile.file.metadata.enums.TSDataType;
import com.corp.delta.tsfile.filter.definition.FilterFactory;
import com.corp.delta.tsfile.filter.definition.SingleSeriesFilterExpression;
import com.corp.delta.tsfile.filter.definition.filterseries.BooleanFilterSeries;
import com.corp.delta.tsfile.filter.definition.filterseries.DoubleFilterSeries;
import com.corp.delta.tsfile.filter.definition.filterseries.FilterSeriesType;
import com.corp.delta.tsfile.filter.definition.filterseries.FloatFilterSeries;
import com.corp.delta.tsfile.filter.definition.filterseries.IntFilterSeries;
import com.corp.delta.tsfile.filter.definition.filterseries.LongFilterSeries;
import com.corp.delta.tsfile.filter.definition.operators.And;
import com.corp.delta.tsfile.filter.definition.operators.Eq;
import com.corp.delta.tsfile.filter.definition.operators.GtEq;
import com.corp.delta.tsfile.filter.definition.operators.LtEq;
import com.corp.delta.tsfile.filter.definition.operators.Not;
import com.corp.delta.tsfile.filter.definition.operators.NotEq;
import com.corp.delta.tsfile.filter.definition.operators.Or;

/**
 *  
 * @author CGF
 *
 */
public class FilterApiMethodsTest {
	private static final Logger LOG = LoggerFactory.getLogger(FilterApiMethodsTest.class);
	
	private static String deltaObjectINT = FilterTestConstant.deltaObjectINT;
	private static String measurementINT = FilterTestConstant.measurementINT;
	private static String deltaObjectLONG = FilterTestConstant.deltaObjectLONG;
	private static String measurementLONG = FilterTestConstant.measurementLONG;
	private static String deltaObjectFLOAT = FilterTestConstant.deltaObjectFLOAT;
	private static String measurementFLOAT = FilterTestConstant.measurementFLOAT;
	private static String deltaObjectDOUBLE = FilterTestConstant.deltaObjectDOUBLE;
	private static String measurementDOUBLE = FilterTestConstant.measurementDOUBLE;
	private static String deltaObjectBOOLEAN = FilterTestConstant.deltaObjectBOOLEAN;
	private static String measurementBOOLEAN = FilterTestConstant.measurementBOOLEAN;
    
    private static final IntFilterSeries intFilterSeries = FilterFactory.intFilterSeries(deltaObjectINT, measurementINT, FilterSeriesType.VALUE_FILTER);
    private static final LongFilterSeries longFilterSeries = FilterFactory.longFilterSeries(deltaObjectLONG, measurementLONG, FilterSeriesType.VALUE_FILTER);
    private static final FloatFilterSeries floatFilterSeries =
            FilterFactory.floatFilterSeries(deltaObjectFLOAT, measurementFLOAT, FilterSeriesType.VALUE_FILTER);
    private static final BooleanFilterSeries booleanFilterSeries =
            FilterFactory.booleanFilterSeries(deltaObjectBOOLEAN, measurementBOOLEAN, FilterSeriesType.VALUE_FILTER); 
    private static final DoubleFilterSeries doubleFilterSeries =
            FilterFactory.doubleFilterSeries(deltaObjectDOUBLE, measurementDOUBLE, FilterSeriesType.VALUE_FILTER);
    
    @Test
    public void testFilterSeriesCreation() {
        assertEquals(intFilterSeries.getDeltaObjectUID(), deltaObjectINT);
        assertEquals(intFilterSeries.getMeasurementUID(), measurementINT);
        assertEquals(intFilterSeries.getSeriesDataType(), TSDataType.INT32);
        
        assertEquals(longFilterSeries.getDeltaObjectUID(), deltaObjectLONG);
        assertEquals(longFilterSeries.getMeasurementUID(), measurementLONG);
        assertEquals(longFilterSeries.getSeriesDataType(), TSDataType.INT64);
        
        assertEquals(floatFilterSeries.getDeltaObjectUID(), deltaObjectFLOAT);
        assertEquals(floatFilterSeries.getMeasurementUID(), measurementFLOAT);
        assertEquals(floatFilterSeries.getSeriesDataType(), TSDataType.FLOAT);
        
        assertEquals(booleanFilterSeries.getDeltaObjectUID(), deltaObjectBOOLEAN);
        assertEquals(booleanFilterSeries.getMeasurementUID(), measurementBOOLEAN);
        assertEquals(booleanFilterSeries.getSeriesDataType(), TSDataType.BOOLEAN);
        
        assertEquals(doubleFilterSeries.getDeltaObjectUID(), deltaObjectDOUBLE);
        assertEquals(doubleFilterSeries.getMeasurementUID(), measurementDOUBLE);
        assertEquals(doubleFilterSeries.getSeriesDataType(), TSDataType.DOUBLE);
         
        assertFalse(intFilterSeries.equals(longFilterSeries));

    }

    @Test
    public void testUnaryOperators() {
        SingleSeriesFilterExpression fe = FilterFactory.eq(intFilterSeries, 15);
        assertTrue(fe instanceof Eq);
        assertEquals(((Eq<?>) fe).getValue(), 15);
        
        SingleSeriesFilterExpression lteq = FilterFactory.ltEq(intFilterSeries, 11, true);
        assertTrue(lteq instanceof LtEq);
        assertEquals(((LtEq<?>) lteq).getValue(), 11);
        
        SingleSeriesFilterExpression gteq = FilterFactory.gtEq(intFilterSeries, 22, true);
        assertTrue(gteq instanceof GtEq);
        assertEquals(((GtEq<?>) gteq).getValue(), 22);
        
        SingleSeriesFilterExpression noteq = FilterFactory.noteq(intFilterSeries, 11);
        assertTrue(noteq instanceof NotEq);
        assertEquals(((NotEq<?>) noteq).getValue(), 11);
        
        SingleSeriesFilterExpression not = FilterFactory.not(noteq);
        assertTrue(not instanceof Not);
        assertEquals(((NotEq<?>) noteq).getValue(), 11);
    }

    @Test
    public void testBinaryOperators() {
        SingleSeriesFilterExpression ltEq = FilterFactory.ltEq(intFilterSeries, 60, true);
        SingleSeriesFilterExpression gtEq = FilterFactory.gtEq(intFilterSeries, 15, true);
        SingleSeriesFilterExpression and = (SingleSeriesFilterExpression) FilterFactory.and(ltEq, gtEq);
        
        assertEquals(((And)and).getLeft(), ltEq);
        assertEquals(((And)and).getRight(), gtEq);
        LOG.info(and.toString());
        
        SingleSeriesFilterExpression or = (SingleSeriesFilterExpression) FilterFactory.or(ltEq, gtEq);
        assertEquals(((Or)or).getLeft(), ltEq);
        assertEquals(((Or)or).getRight(), gtEq);
        LOG.info(or.toString());
    }

    @Test
    public void testFilterCreation() {
        SingleSeriesFilterExpression fe = FilterFactory.eq(intFilterSeries, 15);
        assertTrue(fe instanceof Eq);
        assertEquals(((Eq<?>) fe).getValue(), 15);
    }

}
