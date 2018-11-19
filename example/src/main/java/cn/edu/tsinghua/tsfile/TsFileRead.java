package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.timeseries.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * The class is to show how to read TsFile file named "test.tsfile".
 * The TsFile file "test.tsfile" is generated from class TsFileWrite2 or class TsFileWrite,
 * they generate the same TsFile file by two different ways
 *
 * Run TsFileWrite1 or TsFileWrite to generate the test.tsfile first
 */
public class TsFileRead {

	public static void main(String[] args) throws IOException {

		// file path
		String path = "test.tsfile";

		// read example : no filter
		TsFileSequenceReader reader = new TsFileSequenceReader(path);
		ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
		ArrayList<Path> paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		QueryExpression queryExpression = QueryExpression.create(paths, null);
		QueryDataSet queryDataSet = readTsFile.query(queryExpression);
		while (queryDataSet.hasNext()) {
			System.out.println(queryDataSet.next());
		}
		System.out.println("------------");
		reader.close();

		// time filter : 4 <= time <= 10
		QueryFilter timeFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq( 4L)),
				new GlobalTimeFilter(TimeFilter.ltEq(10L)));
		reader = new TsFileSequenceReader(path);
		readTsFile = new ReadOnlyTsFile(reader);
		paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		queryExpression = QueryExpression.create(paths, timeFilter);
		queryDataSet = readTsFile.query(queryExpression);
		while (queryDataSet.hasNext()) {
			System.out.println(queryDataSet.next());
		}
		System.out.println("------------");
		reader.close();

		// value filter : device_1.sensor_2 <= 20
		QueryFilter valueFilter = new SeriesFilter<>(new Path("device_1.sensor_2"), ValueFilter.ltEq(20));
		reader = new TsFileSequenceReader(path);
		readTsFile = new ReadOnlyTsFile(reader);
		paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		queryExpression = QueryExpression.create(paths, valueFilter);
		queryDataSet = readTsFile.query(queryExpression);
		while (queryDataSet.hasNext()) {
			System.out.println(queryDataSet.next());
		}
		System.out.println("------------");
		reader.close();

		// time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20
		timeFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(4L)),
				new GlobalTimeFilter(TimeFilter.ltEq(10L)));
		valueFilter = new SeriesFilter<>(new Path("device_1.sensor_3"), ValueFilter.gtEq(20));
		reader = new TsFileSequenceReader(path);
		readTsFile = new ReadOnlyTsFile(reader);
		paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));
		paths.add(new Path("device_1.sensor_3"));
		QueryFilter finalFilter = QueryFilterFactory.and(timeFilter, valueFilter);
		queryExpression = QueryExpression.create(paths, finalFilter);
		queryDataSet = readTsFile.query(queryExpression);
		while (queryDataSet.hasNext()) {
			System.out.println(queryDataSet.next());
		}
		reader.close();
	}

}