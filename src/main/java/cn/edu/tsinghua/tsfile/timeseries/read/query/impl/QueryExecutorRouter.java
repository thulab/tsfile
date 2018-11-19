package cn.edu.tsinghua.tsfile.timeseries.read.query.impl;

import cn.edu.tsinghua.tsfile.timeseries.filter.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class QueryExecutorRouter implements QueryExecutor {

    private MetadataQuerier metadataQuerier;
    private SeriesChunkLoader seriesChunkLoader;

    public QueryExecutorRouter(MetadataQuerier metadataQuerier, SeriesChunkLoader seriesChunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.seriesChunkLoader = seriesChunkLoader;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        if (queryExpression.hasQueryFilter()) {
            try {
                QueryFilter queryFilter = queryExpression.getQueryFilter();
                QueryFilter regularQueryFilter = QueryFilterOptimizer.getInstance().convertGlobalTimeFilter(queryFilter, queryExpression.getSelectedSeries());
                queryExpression.setQueryFilter(regularQueryFilter);
                if (regularQueryFilter instanceof GlobalTimeFilter) {
                    return new QueryWithGlobalTimeFilterExecutorImpl(seriesChunkLoader, metadataQuerier).execute(queryExpression);
                } else {
                    return new QueryWithQueryFilterExecutorImpl(seriesChunkLoader, metadataQuerier).execute(queryExpression);
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return new QueryWithoutFilterExecutorImpl(seriesChunkLoader, metadataQuerier).execute(queryExpression);
        }
    }
}
