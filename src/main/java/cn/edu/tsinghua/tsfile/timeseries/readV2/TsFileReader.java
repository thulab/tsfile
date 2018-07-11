package cn.edu.tsinghua.tsfile.timeseries.readV2;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;

import java.io.File;
import java.io.IOException;

public class TsFileReader {
    private File file;
    private ITsRandomAccessFileReader randomAccessFileReader;
    private MetadataQuerier metadataQuerier;
    private SeriesChunkLoader seriesChunkLoader;
    private QueryExecutorRouter queryExecutorRouter;

    public TsFileReader(File file){
        this.file=file;

    }
    public void open()  throws IOException {
        this.randomAccessFileReader = new TsRandomAccessLocalFileReader(file.getAbsolutePath());
        this.metadataQuerier = new MetadataQuerierByFileImpl(randomAccessFileReader);
        this.seriesChunkLoader = new SeriesChunkLoaderImpl(randomAccessFileReader);
        queryExecutorRouter = new QueryExecutorRouter(metadataQuerier, seriesChunkLoader);
    }


    public QueryDataSet query(QueryExpression queryExpression) throws IOException {
        return queryExecutorRouter.execute(queryExpression);
    }

    public void close() throws IOException {
        randomAccessFileReader.close();
    }

   
}
