package com.corp.delta.tsfile.filter.visitorImpl;

import com.corp.delta.tsfile.filter.definition.operators.And;
import com.corp.delta.tsfile.filter.definition.operators.Eq;
import com.corp.delta.tsfile.filter.definition.operators.GtEq;
import com.corp.delta.tsfile.filter.definition.operators.LtEq;
import com.corp.delta.tsfile.filter.definition.operators.Not;
import com.corp.delta.tsfile.filter.definition.operators.NotEq;
import com.corp.delta.tsfile.filter.definition.operators.Or;

/**
 * FilterVistor is implemented by visitor pattern.
 * Implemented using visitor pattern.
 * 
 * @author CGF
 *
 */
public interface FilterVisitor<R> {

    /**
     * A FilterVistor must visit all these methods below, per visitor design pattern. 
     * And a FilterExpression just need implements an accept() method.
     */

    <T extends Comparable<T>> R visit(Eq<T> eq);

    <T extends Comparable<T>> R visit(NotEq<T> notEq);

    <T extends Comparable<T>> R visit(LtEq<T> ltEq);

    <T extends Comparable<T>> R visit(GtEq<T> gtEq);

    R visit(Not not);

    R visit(And and);

    R visit(Or or);

}
