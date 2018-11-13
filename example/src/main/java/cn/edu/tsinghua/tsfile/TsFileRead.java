/**
 * The class is to show how to read TsFile file named "test.tsfile".
 * The TsFile file "test.tsfile" is generated from class TsFileWrite1 or class TsFileWrite,
 * they generate the same TsFile file by two different ways
 */
package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;

import java.io.IOException;
import java.util.ArrayList;

/**
 * An example of reading data from TsFile
 *
 * Run TsFileWrite1 or TsFileWrite to generate the test.ts first
 */
public class TsFileRead {

	public static void main(String[] args) throws IOException {

		// file path
		String path = "test.tsfile";

		// read example : no filter
		TsFileSequenceReader input = new TsFileSequenceReader(path);
		input.open();
		ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(input);
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
		input.close();

		// time filter : 4 <= time <= 10
		QueryFilter timeFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq( 4L)),
				new GlobalTimeFilter(TimeFilter.ltEq(10L)));
		input = new TsFileSequenceReader(path);
		input.open();
		readTsFile = new ReadOnlyTsFile(input);
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
		input.close();

		// value filter : device_1.sensor_2 <= 20
		QueryFilter valueFilter = new SeriesFilter<>(new Path("device_1.sensor_2"), ValueFilter.ltEq(20));
		input = new TsFileSequenceReader(path);
		input.open();
		readTsFile = new ReadOnlyTsFile(input);
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
		input.close();

		// time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20
		timeFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(4L)),
				new GlobalTimeFilter(TimeFilter.ltEq(10L)));
		valueFilter = new SeriesFilter<>(new Path("device_1.sensor_3"), ValueFilter.gtEq(20));
		input = new TsFileSequenceReader(path);
		input.open();
		readTsFile = new ReadOnlyTsFile(input);
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
		input.close();
	}

}