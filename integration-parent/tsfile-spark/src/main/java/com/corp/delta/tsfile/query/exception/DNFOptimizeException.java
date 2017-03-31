package com.corp.delta.tsfile.query.exception;

import com.corp.delta.tsfile.query.optimizer.DNFFilterOptimizer;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain DNFFilterOptimizer}
 *
 */
public class DNFOptimizeException extends LogicalOptimizeException {

    private static final long serialVersionUID = 807384397361662482L;

    public DNFOptimizeException(String msg) {
        super(msg);
    }

}
