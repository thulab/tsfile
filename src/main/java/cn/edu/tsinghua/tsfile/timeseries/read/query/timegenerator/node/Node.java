package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public interface Node {

    boolean hasNext() throws IOException;

    long next() throws IOException;

    NodeType getType();
}
