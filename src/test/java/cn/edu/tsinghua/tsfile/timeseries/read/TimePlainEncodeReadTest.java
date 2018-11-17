package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimePlainEncodeReadTest {

	private static String fileName = "src/test/resources/perTestOutputData.tsfile";
	private static ReadOnlyTsFile roTsFile = null;

	@Before
	public void prepare() throws IOException, InterruptedException, WriteProcessException {
		TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "PLAIN";
		ReadPerf.generateFile();
		TsFileSequenceReader reader = new TsFileSequenceReader(fileName);
		roTsFile = new ReadOnlyTsFile(reader);
	}

	@After
	public void after() throws IOException {
		if (roTsFile != null)
			roTsFile.close();
		ReadPerf.after();
	}

	@Test
	public void queryOneMeasurementWithoutFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s1"));
		QueryExpression queryExpression = QueryExpression.create(pathList, null);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int count = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (count == 0) {
				assertEquals(r.getTimestamp(), 1480562618010L);
			}
			if (count == 499) {
				assertEquals(r.getTimestamp(), 1480562618999L);
			}
			count++;
		}
		assertEquals(count, 500);
	}

	@Test
	public void queryTwoMeasurementsWithoutFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s1"));
		pathList.add(new Path("d2.s2"));
		QueryExpression queryExpression = QueryExpression.create(pathList, null);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int count = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (count == 0) {
				if (count == 0) {
					assertEquals(1480562618005L, r.getTimestamp());
				}
			}
			count++;
		}
		assertEquals(count, 750);
	}

	@Test
	public void queryTwoMeasurementsWithSingleFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d2.s1"));
		pathList.add(new Path("d2.s4"));
		QueryFilter valFilter = new SeriesFilter<>(new Path("d2.s2"), ValueFilter.gt(9722L));
		QueryFilter tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeFilter(TimeFilter.lt(1480562618977L)));
		QueryFilter finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
		}

	}

	@Test
	public void queryWithTwoSeriesTimeValueFilterCrossTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d2.s2"));
		QueryFilter valFilter = new SeriesFilter<>(new Path("d2.s2"), ValueFilter.notEq(9722L));
		QueryFilter tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeFilter(TimeFilter.lt(1480562618977L)));
		QueryFilter finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		// time filter & value filter
		// verify d1.s1, d2.s1
		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618970L);
			} else if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618971L);
			} else if (cnt == 3) {
				assertEquals(r.getTimestamp(), 1480562618973L);
			}
			//System.out.println(r);
			cnt++;
		}
		assertEquals(cnt, 7);
	}

	@Test
	public void queryWithCrossSeriesTimeValueFilterTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s1"));
		pathList.add(new Path("d2.s2"));
		QueryFilter valFilter = new SeriesFilter<>(new Path("d2.s2"), ValueFilter.gt(9722L));
		QueryFilter tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeFilter(TimeFilter.lt(1480562618977L)));
		QueryFilter finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		// time filter & value filter
		// verify d1.s1, d2.s1
		/**
		 1480562618950	9501	9502
		 1480562618954	9541	9542
		 1480562618955	9551	9552
		 1480562618956	9561	9562
		 */
		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618973L);
			} else if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618974L);
			} else if (cnt == 3) {
				assertEquals(r.getTimestamp(), 1480562618975L);
			} else if (cnt == 4) {
				assertEquals(r.getTimestamp(), 1480562618976L);
			}
			//System.out.println(r);
			cnt++;
		}
		assertEquals(cnt, 5);

		pathList.clear();
		pathList.add(new Path("d1.s1"));
		pathList.add(new Path("d2.s2"));
		valFilter = new SeriesFilter<>(new Path("d1.s1"), ValueFilter.ltEq(9321));
		valFilter = QueryFilterFactory.and(new SeriesFilter<>(new Path("d2.s2"), ValueFilter.ltEq(9312L)),
				valFilter);
		tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618906L)),
				new GlobalTimeFilter(TimeFilter.ltEq(1480562618915L)));
		tFilter = QueryFilterFactory.or(tFilter,
				QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618928L)),
						new GlobalTimeFilter(TimeFilter.ltEq(1480562618933L))));
		finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		queryExpression = QueryExpression.create(pathList, finalFilter);
		dataSet = roTsFile.query(queryExpression);

		// time filter & value filter
		// verify d1.s1, d2.s1
		/**
		 1480562618910	9101	9102
		 1480562618911	9111	9112
		 1480562618912	9121	9122
		 1480562618913	9131	9132
		 1480562618914	9141	9142
		 1480562618915	9151	9152
		 1480562618930	9301	9302
		 1480562618931	9311	9312
		 1480562618932	9321	9322
		 1480562618933	9331	9332
		 */
		cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			//System.out.println(r);
			if (cnt == 4) {
				assertEquals(r.getTimestamp(), 1480562618913L);
			} else if (cnt == 7) {
				assertEquals(r.getTimestamp(), 1480562618930L);
			}
			cnt++;
		}
		assertEquals(cnt, 9);
	}

	@Test
	public void queryBooleanTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s5"));
		QueryFilter valFilter = new SeriesFilter<>(new Path("d1.s5"), ValueFilter.eq(false));
		QueryFilter tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeFilter(TimeFilter.lt(1480562618981L)));
		QueryFilter finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			System.out.println(r);
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618972L);
				TsPrimitiveType f1 = r.getFields().get(new Path("d1.s5"));
				assertEquals(f1.getBoolean(), false);
			}
			if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618981L);
				TsPrimitiveType f2 = r.getFields().get(new Path("d1.s5"));
				assertEquals(f2.getBoolean(), false);
			}
			cnt++;
		}
	}

	@Test
	public void queryStringTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s4"));
		QueryFilter valFilter = new SeriesFilter<>(new Path("d1.s4"), ValueFilter.gt(new Binary("dog97")));
		QueryFilter tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeFilter(TimeFilter.ltEq(1480562618981L)));
		QueryFilter finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 0) {
				assertEquals(r.getTimestamp(), 1480562618976L);
				TsPrimitiveType f1 = r.getFields().get(new Path("d1.s4"));
				assertEquals(f1.getStringValue(), "dog976");
			}
			// System.out.println(r);
			cnt++;
		}
		Assert.assertEquals(cnt, 1);

		pathList = new ArrayList<>();
		pathList.add(new Path("d1.s4"));
		valFilter = new SeriesFilter<>(new Path("d1.s4"), ValueFilter.lt(new Binary("dog97")));
		tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeFilter(TimeFilter.ltEq(1480562618981L)));
		finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		queryExpression = QueryExpression.create(pathList, finalFilter);
		dataSet = roTsFile.query(queryExpression);
		cnt = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618976L);
				TsPrimitiveType f1 = r.getFields().get(new Path("d1.s4"));
				assertEquals(f1.getBinary(), "dog976");
			}
			// System.out.println(r);
			cnt++;
		}
		Assert.assertEquals(cnt, 0);

	}

	@Test
	public void queryFloatTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s6"));
		QueryFilter valFilter = new SeriesFilter<>(new Path("d1.s6"), ValueFilter.gt(103.0f));
		QueryFilter tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618970L)),
				new GlobalTimeFilter(TimeFilter.ltEq(1480562618981L)));
		QueryFilter finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 0;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618980L);
				TsPrimitiveType f1 = r.getFields().get(new Path("d1.s6"));
				assertEquals(f1.getFloat(), 108.0, 0.0);
			}
			if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618990L);
				TsPrimitiveType f2 = r.getFields().get(new Path("d1.s6"));
				assertEquals(f2.getFloat(), 110.0, 0.0);
			}
			cnt++;
		}
	}

	@Test
	public void queryDoubleTest() throws IOException {
		List<Path> pathList = new ArrayList<>();
		pathList.add(new Path("d1.s7"));
		QueryFilter valFilter = new SeriesFilter<>(new Path("d1.s7"), ValueFilter.gt(7.0));
		QueryFilter tFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(1480562618021L)),
				new GlobalTimeFilter(TimeFilter.ltEq(1480562618033L)));
		QueryFilter finalFilter = QueryFilterFactory.and(valFilter, tFilter);
		QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
		QueryDataSet dataSet = roTsFile.query(queryExpression);

		int cnt = 1;
		while (dataSet.hasNext()) {
			RowRecord r = dataSet.next();
			if (cnt == 1) {
				assertEquals(r.getTimestamp(), 1480562618022L);
				TsPrimitiveType f1 = r.getFields().get(0);
				assertEquals(f1.getDouble(), 2.0, 0.0);
			}
			if (cnt == 2) {
				assertEquals(r.getTimestamp(), 1480562618033L);
				TsPrimitiveType f1 = r.getFields().get(0);
				assertEquals(f1.getDouble(), 3.0, 0.0);
			}
			cnt++;
		}
	}
}
