package com.corp.delta.tsfile.filter.definition.filterseries;

import com.corp.delta.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of FloatFilterSeries is Float.
 * 
 * @author CGF
 *
 */
public class FloatFilterSeries extends FilterSeries<Float> {

	private static final long serialVersionUID = -2745416005497409478L;

	public FloatFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
			FilterSeriesType filterType) {
		super(deltaObjectUID, measurementUID, TSDataType.FLOAT, filterType);
	}
}
