package com.corp.delta.tsfile.filter.definition.filterseries;

import com.corp.delta.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of LongFilterSeries is Long.
 * 
 * @author CGF
 *
 */
public class LongFilterSeries extends FilterSeries<Long> {

	private static final long serialVersionUID = -6805221044991568903L;

	public LongFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
			FilterSeriesType filterType) {
		super(deltaObjectUID, measurementUID, TSDataType.INT64, filterType);
	}
}
