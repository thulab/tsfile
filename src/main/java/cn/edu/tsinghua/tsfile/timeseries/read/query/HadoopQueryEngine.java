package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.RecordReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HadoopQueryEngine extends QueryEngine {

    private static final String SEPARATOR_DEVIDE_SERIES = ".";
    private List<RowGroupMetaData> rowGroupMetaDataList;

    public HadoopQueryEngine(ITsRandomAccessFileReader raf, List<RowGroupMetaData> rowGroupMetaDataList) throws IOException {
        super(raf, rowGroupMetaDataList);
        this.rowGroupMetaDataList = rowGroupMetaDataList;
    }

    public QueryDataSet queryWithSpecificRowGroups(List<String> deviceIdList, List<MeasurementDescriptor> sensorList, FilterExpression timeFilter, FilterExpression freqFilter, FilterExpression valueFilter) throws IOException{
        List<Path> paths = new ArrayList<>();
        for(String deviceId : deviceIdList){
            for(MeasurementDescriptor sensor: sensorList){
                paths.add(new Path(deviceId + SEPARATOR_DEVIDE_SERIES + sensor.getMeasurementId()));
            }
        }

        if (timeFilter == null && freqFilter == null && valueFilter == null) {
            return queryWithoutFilter(paths);
        } else if (valueFilter instanceof SingleSeriesFilterExpression || (timeFilter != null && valueFilter == null)) {
//            return readOneColumnValueUseFilter(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
//                    (SingleSeriesFilterExpression) valueFilter);
        } else if (valueFilter instanceof CrossSeriesFilterExpression) {
//            return crossColumnQuery(paths, (SingleSeriesFilterExpression) timeFilter, (SingleSeriesFilterExpression) freqFilter,
//                    (CrossSeriesFilterExpression) valueFilter, rowGroupIndexList);
        }
        throw new IOException("Query Not Support Exception");
    }

    private QueryDataSet queryWithoutFilter(List<Path> paths) throws IOException {
        return new IteratorQueryDataSet(paths) {
            @Override
            public DynamicOneColumnData getMoreRecordsForOneColumn(Path p, DynamicOneColumnData res) throws IOException {
                return recordReader.getValueInOneColumn(res, FETCH_SIZE, p.getDeltaObjectToString(), p.getMeasurementToString());
            }
        };
    }
}
