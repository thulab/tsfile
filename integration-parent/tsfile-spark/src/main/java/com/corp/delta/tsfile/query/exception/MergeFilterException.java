package com.corp.delta.tsfile.query.exception;

import com.corp.delta.tsfile.query.optimizer.MergeSingleFilterOptimizer;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain MergeSingleFilterOptimizer}
 *
 */
public class MergeFilterException extends LogicalOptimizeException {

    private static final long serialVersionUID = 8581594261924961899L;

    public MergeFilterException(String msg) {
        super(msg);
    }

}
