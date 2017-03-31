package com.corp.delta.tsfile.filter.definition.filterseries;

import com.corp.delta.tsfile.file.metadata.enums.TSDataType;

/**
 * The value type of BooleanFilterSeries is Boolean.
 * 
 * @author CGF
 *
 */
public class BooleanFilterSeries extends FilterSeries<Boolean> {

	private static final long serialVersionUID = 454794989741185890L;

	public BooleanFilterSeries(String deltaObjectUID, String measurementUID, TSDataType seriesDataType,
			FilterSeriesType filterType) {
		super(deltaObjectUID, measurementUID, TSDataType.BOOLEAN, filterType);
	}
}
