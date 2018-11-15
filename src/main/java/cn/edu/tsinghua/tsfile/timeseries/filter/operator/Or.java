package cn.edu.tsinghua.tsfile.timeseries.filter.operator;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.BinaryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.AbstractFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.io.Serializable;

/**
 * Either of the left and right operators of And must satisfy the condition.
 *
 * @author CGF
 */
public class Or<T extends Comparable<T>> extends BinaryFilter<T> implements Serializable {

    private static final long serialVersionUID = -968055896528472694L;

    public Or(Filter left, Filter right) {
        super(left, right);
    }

    @Override
    public String toString() {
        return "(" + left + " || " + right + ")";
    }

    @Override
    public <R> R accept(AbstractFilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public <R> R accept(TimeValuePair value, TimeValuePairFilterVisitor<R> visitor) {
        return visitor.visit(value, this);
    }
}
