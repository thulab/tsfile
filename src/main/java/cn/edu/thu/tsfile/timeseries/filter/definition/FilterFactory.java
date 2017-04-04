package cn.edu.thu.tsfile.timeseries.filter.definition;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.LongFilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.BooleanFilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.DoubleFilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FloatFilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.IntFilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.StringFilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.CSAnd;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.CSOr;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Or;

/**
 * The FilterFactory is used to construct FilterSeries, SingleSeriesFilter, and
 * CrossSeriesFilter.</br>
 * 
 * @author CGF
 *
 */
public final class FilterFactory {

	/**
	 * To construct Time FilterSeries
	 *
	 * @return
	 */
	public static LongFilterSeries timeFilterSeries() {
		return new LongFilterSeries(null, null, TSDataType.INT64, FilterSeriesType.TIME_FILTER);
	}

	/**
	 * To construct IntFilterSeries
	 * 
	 * @param deltaObjectUID
	 * @param measurementUID
	 * @param filterType
	 * @return
	 */
	public static IntFilterSeries intFilterSeries(String deltaObjectUID, String measurementUID,
			FilterSeriesType filterType) {
		return new IntFilterSeries(deltaObjectUID, measurementUID, TSDataType.INT32, filterType);
	}

	/**
	 * To construct DoubleFilterSeries
	 * 
	 * @param deltaObjectUID
	 * @param measurementUID
	 * @param filterType
	 * @return
	 */
	public static DoubleFilterSeries doubleFilterSeries(String deltaObjectUID, String measurementUID,
			FilterSeriesType filterType) {
		return new DoubleFilterSeries(deltaObjectUID, measurementUID, TSDataType.DOUBLE, filterType);
	}

	/**
	 * To construct LongFilterSeries
	 * 
	 * @param deltaObjectUID
	 * @param measurementUID
	 * @param filterType
	 * @return
	 */
	public static LongFilterSeries longFilterSeries(String deltaObjectUID, String measurementUID,
			FilterSeriesType filterType) {
		return new LongFilterSeries(deltaObjectUID, measurementUID, TSDataType.INT64, filterType);
	}

	/**
	 * To construct FloatFilterSeries
	 * 
	 * @param deltaObjectUID
	 * @param measurementUID
	 * @param filterType
	 * @return
	 */
	public static FloatFilterSeries floatFilterSeries(String deltaObjectUID, String measurementUID,
			FilterSeriesType filterType) {
		return new FloatFilterSeries(deltaObjectUID, measurementUID, TSDataType.FLOAT, filterType);
	}

	/**
	 * To construct BooleanFilterSeries
	 * 
	 * @param deltaObjectUID
	 * @param measurementUID
	 * @param filterType
	 * @return
	 */
	public static BooleanFilterSeries booleanFilterSeries(String deltaObjectUID, String measurementUID,
			FilterSeriesType filterType) {
		return new BooleanFilterSeries(deltaObjectUID, measurementUID, TSDataType.BOOLEAN, filterType);
	}

	/**
	 * To construct StringFilterSeries
	 * 
	 * @param deltaObjectUID
	 * @param measurementUID
	 * @param filterType
	 * @return
	 */
	public static StringFilterSeries stringFilterSeries(String deltaObjectUID, String measurementUID,
			FilterSeriesType filterType) {
		return new StringFilterSeries(deltaObjectUID, measurementUID, TSDataType.BYTE_ARRAY, filterType);
	}
	
	/**
	 * To generate Eq by filterSeries
	 * 
	 * @param filterSeries
	 * @param value
	 * @return
	 */
	public static <T extends Comparable<T>, C extends FilterSeries<T>> Eq<T> eq(C filterSeries, T value) {
		return new Eq<T>(filterSeries, value);
	}

	/**
	 * To generate LtEq by filterSeries
	 * 
	 * @param filterSeries
	 * @param value
	 * @param ifEq
	 * @return
	 */
	public static <T extends Comparable<T>, C extends FilterSeries<T>> LtEq<T> ltEq(C filterSeries, T value,
																					Boolean ifEq) {
		return new LtEq<T>(filterSeries, value, ifEq);
	}

	/**
	 * To generate GtEq by filterSeries
	 * 
	 * @param filterSeries
	 * @param value
	 * @param ifEq
	 * @return
	 */
	public static <T extends Comparable<T>, C extends FilterSeries<T>> GtEq<T> gtEq(C filterSeries, T value,
																					Boolean ifEq) {
		return new GtEq<T>(filterSeries, value, ifEq);
	}

	/**
	 * To generate NotEq by filterSeries
	 * 
	 * @param filterSeries
	 * @param value
	 * @return
	 */
	public static <T extends Comparable<T>, C extends FilterSeries<T>> NotEq<T> noteq(C filterSeries, T value) {
		return new NotEq<T>(filterSeries, value);
	}

	/**
	 * To generate Not by filterSeries
	 *
	 * @param that
	 * @return
	 */
	public static SingleSeriesFilterExpression not(SingleSeriesFilterExpression that) {
		return new Not(that);
	}
	/**
	 * To generate And by filterSeries
	 *
	 * @param left
	 * @param right
	 * @return
	 */
	private static SingleSeriesFilterExpression ssAnd(SingleSeriesFilterExpression left, SingleSeriesFilterExpression right) {
//		if (left.getFilterSeries().sameSeries(right.getFilterSeries()))
		return new And(left, right);
//		else
//			return new CSAnd(left, right);
	}

	/**
	 * To generate Or by filterSeries
	 *
	 * @param left
	 * @param right
	 * @return
	 */
	private static SingleSeriesFilterExpression ssOr(SingleSeriesFilterExpression left, SingleSeriesFilterExpression right) {
//		if (left.getFilterSeries().sameSeries(right.getFilterSeries()))
		return new Or(left, right);
//		else
//			return new CSOr(left, right);
	}

	/**
	 * construct CSAnd(Cross Series Filter And Operators) use FilterExpression
	 * left, right;
	 *
	 * @param left
	 * @param right
	 * @return
	 */
	public static CSAnd csAnd(FilterExpression left, FilterExpression right) {
		return new CSAnd(left, right);
	}

	/**
	 * construct CSOr(Cross Series Filter Or Operators) use FilterExpression
	 * left, right;
	 *
	 * @param left
	 * @param right
	 * @return
	 */
	private static CSOr csOr(FilterExpression left, FilterExpression right) {
		return new CSOr(left, right);
	}

	public static FilterExpression and (FilterExpression left, FilterExpression right) {
		if (left.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER &&
				right.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER) {
			return ssAnd((SingleSeriesFilterExpression)left, (SingleSeriesFilterExpression)right);
		}

		if (left instanceof SingleSeriesFilterExpression && right instanceof SingleSeriesFilterExpression
				&& (((SingleSeriesFilterExpression) left).getFilterSeries().sameSeries(((SingleSeriesFilterExpression) right).getFilterSeries()))) {
			return ssAnd((SingleSeriesFilterExpression)left, (SingleSeriesFilterExpression) right);
		} else {
			return csAnd(left, right);
		}
	}

	public static FilterExpression or (FilterExpression left, FilterExpression right) {
		if (left.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER &&
				right.getFilterSeries().getFilterType() == FilterSeriesType.TIME_FILTER) {
			return ssOr((SingleSeriesFilterExpression)left, (SingleSeriesFilterExpression)right);
		}

		if (left instanceof SingleSeriesFilterExpression && right instanceof SingleSeriesFilterExpression
				&& (((SingleSeriesFilterExpression) left).getFilterSeries().sameSeries(((SingleSeriesFilterExpression) right).getFilterSeries()))) {
			return ssOr((SingleSeriesFilterExpression)left, (SingleSeriesFilterExpression) right);
		} else {
			return csOr(left, right);
		}
	}
}
