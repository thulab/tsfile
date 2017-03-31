package com.corp.delta.tsfile.query.optimizer;

import com.corp.delta.tsfile.query.common.FilterOperator;
import com.corp.delta.tsfile.query.exception.*;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
