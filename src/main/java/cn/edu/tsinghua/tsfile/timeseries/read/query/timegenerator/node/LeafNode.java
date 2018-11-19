package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node;

import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReader;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public class LeafNode implements Node {

    private SeriesReader seriesReader;

    public LeafNode(SeriesReader seriesReader) {
        this.seriesReader = seriesReader;
    }

    @Override
    public boolean hasNext() throws IOException {
        return seriesReader.hasNext();
    }

    @Override
    public long next() throws IOException {
        return seriesReader.next().getTimestamp();
    }

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }
}
