package cn.edu.tsinghua.tsfile.timeseries.readV2.basis;

import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class ReadOnlyTsFile {

    //private ITsRandomAccessFileReader randomAccessFileReader;
    private TsFileSequenceReader fileReader;

    private MetadataQuerier metadataQuerier;
    private SeriesChunkLoader seriesChunkLoader;
    private QueryExecutorRouter queryExecutorRouter;

    public ReadOnlyTsFile(TsFileSequenceReader fileReader) throws IOException {
        this.fileReader = fileReader;
        this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
        this.seriesChunkLoader = new SeriesChunkLoaderImpl(fileReader);
        queryExecutorRouter = new QueryExecutorRouter(metadataQuerier, seriesChunkLoader);
    }

    public QueryDataSet query(QueryExpression queryExpression) throws IOException {
        return queryExecutorRouter.execute(queryExpression);
    }

    public void close() throws IOException {
        fileReader.close();
    }
}
