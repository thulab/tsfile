package com.corp.delta.tsfile.query.exception;


import com.corp.delta.tsfile.query.optimizer.RemoveNotOptimizer;

/**
 * This exception is threw whiling meeting error in
 * {@linkplain RemoveNotOptimizer}
 *
 */
public class RemoveNotException extends LogicalOptimizeException {

    private static final long serialVersionUID = -772591029262375715L;

    public RemoveNotException(String msg) {
        super(msg);
    }

}
