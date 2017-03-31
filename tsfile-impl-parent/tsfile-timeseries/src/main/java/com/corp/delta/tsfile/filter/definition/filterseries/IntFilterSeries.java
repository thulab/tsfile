package com.corp.delta.tsfile.filter.definition.filterseries;

import com.corp.delta.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of IntFilterSeries is Integer.
 * 
 * @author CGF
 *
 */
public class IntFilterSeries extends FilterSeries<Integer> {

	private static final long serialVersionUID = -7268852368134017134L;

	public IntFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
			FilterSeriesType filterType) {
		super(deltaObjectUID, measurementUID, TSDataType.INT32, filterType);
	}
}