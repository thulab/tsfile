package cn.edu.tsinghua.tsfile.file.metadata.utils;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.header.DataPageHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;

public class Utils {
	public static void isListEqual(List<?> listA, List<?> listB, String name) {
		if ((listA == null) ^ (listB == null)) {
			System.out.println("error");
			fail(String.format("one of %s is null", name));
		}
		if ((listA != null) && (listB != null)) {
			if (listA.size() != listB.size()) {
				fail(String.format("%s size is different", name));
			}
			for (int i = 0; i < listA.size(); i++) {
				assertTrue(listA.get(i).equals(listB.get(i)));
			}
		}
	}

	public static void isMapStringEqual(Map<String, String> mapA, Map<String, String> mapB, String name) {
		if ((mapA == null) ^ (mapB == null)) {
			System.out.println("error");
			fail(String.format("one of %s is null", name));
		}
		if ((mapA != null) && (mapB != null)) {
			if (mapA.size() != mapB.size()) {
				fail(String.format("%s size is different", name));
			}
			for (String key : mapA.keySet()) {
				assertTrue(mapA.get(key).equals(mapB.get(key)));
			}
		}
	}

	public static void isMapBufferEqual(Map<String, ByteBuffer> mapA, Map<String, ByteBuffer> mapB, String name) {
        if ((mapA == null) ^ (mapB == null)) {
            System.out.println("error");
            fail(String.format("one of %s is null", name));
        }
        if ((mapA != null) && (mapB != null)) {
            if (mapA.size() != mapB.size()) {
                fail(String.format("%s size is different", name));
            }
            for (String key : mapB.keySet()) {
                ByteBuffer b = mapB.get(key);
                ByteBuffer a = mapA.get(key);
                assertTrue(b.equals(a));
            }
        }
    }


	/**
	 * when one of A and B is Null, A != B, so test case fails.
	 * 
	 * @param objectA
	 * @param objectB
	 * @param name
	 * @return false - A and B both are NULL, so we do not need to check whether
	 *         their members are equal
	 * @return true - A and B both are not NULL, so we need to check their members
	 */
	public static boolean isTwoObjectsNotNULL(Object objectA, Object objectB, String name) {
		if ((objectA == null) && (objectB == null))
			return false;
		if ((objectA == null) ^ (objectB == null))
			fail(String.format("one of %s is null", name));
		return true;
	}

	public static void isStringSame(Object str1, Object str2, String name) {
		if ((str1 == null) && (str2 == null))
			return;
		if ((str1 == null) ^ (str2 == null))
			fail(String.format("one of %s string is null", name));
		assertTrue(str1.toString().equals(str2.toString()));
	}

	public static void isTimeSeriesEqual(TimeSeriesMetadata timeSeriesMetadata1, TimeSeriesMetadata timeSeriesMetadata2) {
		if (Utils.isTwoObjectsNotNULL(timeSeriesMetadata1.getMeasurementUID(), timeSeriesMetadata2.getMeasurementUID(),
				"sensorUID")) {
			assertTrue(timeSeriesMetadata1.getMeasurementUID().equals(timeSeriesMetadata2.getMeasurementUID()));
		}
		assertTrue(timeSeriesMetadata1.getTypeLength() == timeSeriesMetadata2.getTypeLength());
		if (Utils.isTwoObjectsNotNULL(timeSeriesMetadata1.getType(), timeSeriesMetadata2.getType(), "data type")) {
			assertTrue(timeSeriesMetadata1.getType().toString() == timeSeriesMetadata2.getType().toString());
		}
		if (Utils.isTwoObjectsNotNULL(timeSeriesMetadata1.getFreqType(), timeSeriesMetadata2.getFreqType(), "freq type")) {
			assertTrue(timeSeriesMetadata1.getFreqType().toString() == timeSeriesMetadata2.getFreqType().toString());
		}

		Utils.isListEqual(timeSeriesMetadata1.getFrequencies(), timeSeriesMetadata2.getFrequencies(), "frequencies");
		Utils.isListEqual(timeSeriesMetadata1.getEnumValues(), timeSeriesMetadata2.getEnumValues(), "data values");
	}

	public static void isTimeSeriesListEqual(List<TimeSeriesMetadata> timeSeriesMetadataList1,
											 List<TimeSeriesMetadata> timeSeriesMetadataList2, int nn) {
		if (timeSeriesMetadataList1 == null && timeSeriesMetadataList2 == null)
			return;

		if (timeSeriesMetadataList1 == null && timeSeriesMetadataList2 == null)
			return;
		if ((timeSeriesMetadataList1 == null) ^ (timeSeriesMetadataList2 == null))
			fail("one list is null");
		if (timeSeriesMetadataList1.size() != timeSeriesMetadataList2.size())
			fail("list size is different");
		for (int i = 0; i < timeSeriesMetadataList1.size(); i++) {
			isTimeSeriesEqual(timeSeriesMetadataList1.get(i), timeSeriesMetadataList2.get(i));
		}
	}

	public static void isTSeriesChunkMetadataEqual(TInTimeSeriesChunkMetaData tSeriesMetaData1,
												   TInTimeSeriesChunkMetaData tSeriesMetaData2) {
		if (Utils.isTwoObjectsNotNULL(tSeriesMetaData1, tSeriesMetaData2,
				"TimeInTimeSeriesChunkMetaData")) {
			Utils.isStringSame(tSeriesMetaData1.getDataType(), tSeriesMetaData2.getDataType(),
					"data type");
			Utils.isStringSame(tSeriesMetaData1.getFreqType(), tSeriesMetaData2.getFreqType(),
					"freq type");
			assertTrue(tSeriesMetaData1.getStartTime() == tSeriesMetaData2.getStartTime());
			assertTrue(tSeriesMetaData1.getEndTime() == tSeriesMetaData2.getEndTime());
			Utils.isListEqual(tSeriesMetaData1.getFrequencies(), tSeriesMetaData2.getFrequencies(),
					"frequencies");
			Utils.isListEqual(tSeriesMetaData1.getEnumValues(), tSeriesMetaData2.getEnumValues(),
					"data values");
		}
	}

	public static void isDeltaObjectEqual(TsDeltaObject deltaObject1, TsDeltaObject deltaObject2) {
		if (Utils.isTwoObjectsNotNULL(deltaObject1, deltaObject2, "Delta object")) {
			assertTrue(deltaObject1.offset == deltaObject2.offset);
			assertTrue(deltaObject1.metadataBlockSize == deltaObject2.metadataBlockSize);
			assertTrue(deltaObject1.startTime == deltaObject2.startTime);
			assertTrue(deltaObject1.endTime == deltaObject2.endTime);
		}
	}

	public static void isVSeriesChunkMetadataEqual(VInTimeSeriesChunkMetaData vSeriesMetaData1,
												   VInTimeSeriesChunkMetaData vSeriesMetaData2) {
		if (Utils.isTwoObjectsNotNULL(vSeriesMetaData1, vSeriesMetaData2,
				"ValueInTimeSeriesChunkMetaData")) {
			assertTrue(vSeriesMetaData1.getMaxError() == vSeriesMetaData2.getMaxError());
			assertTrue(vSeriesMetaData1.getDataType().toString()
					.equals(vSeriesMetaData2.getDataType().toString()));
			if (Utils.isTwoObjectsNotNULL(vSeriesMetaData1.getDigest(), vSeriesMetaData2.getDigest(),
					"Digest")) {
				Utils.isMapBufferEqual(vSeriesMetaData1.getDigest().getStatistics(),
						vSeriesMetaData2.getDigest().getStatistics(),
						"Diges statistics map");
			}
			Utils.isListEqual(vSeriesMetaData1.getEnumValues(), vSeriesMetaData2.getEnumValues(),
					"data values");
		}
	}

	public static void isTimeSeriesChunkMetaDataEqual(TimeSeriesChunkMetaData timeSeriesChunkMetaData1,
													  TimeSeriesChunkMetaData timeSeriesChunkMetaData2) {
		if (Utils.isTwoObjectsNotNULL(timeSeriesChunkMetaData1, timeSeriesChunkMetaData2,
				"TimeSeriesChunkMetaData")) {
			assertTrue(timeSeriesChunkMetaData1.getProperties().getMeasurementUID()
					.equals(timeSeriesChunkMetaData2.getProperties().getMeasurementUID()));
			assertTrue(timeSeriesChunkMetaData1.getProperties().getTsChunkType().toString()
					.equals(timeSeriesChunkMetaData2.getProperties().getTsChunkType().toString()));
			assertTrue(timeSeriesChunkMetaData1.getProperties().getFileOffset() == timeSeriesChunkMetaData2
					.getProperties().getFileOffset());
			assertTrue(timeSeriesChunkMetaData1.getProperties().getCompression().toString()
					.equals(timeSeriesChunkMetaData2.getProperties().getCompression().toString()));

			assertTrue(timeSeriesChunkMetaData1.getNumRows() == timeSeriesChunkMetaData2.getNumRows());
			assertTrue(timeSeriesChunkMetaData1.getTotalByteSize() == timeSeriesChunkMetaData2
					.getTotalByteSize());
			assertTrue(timeSeriesChunkMetaData1.getDataPageOffset() == timeSeriesChunkMetaData2
					.getDataPageOffset());
			assertTrue(timeSeriesChunkMetaData1.getDictionaryPageOffset() == timeSeriesChunkMetaData2
					.getDictionaryPageOffset());
			assertTrue(timeSeriesChunkMetaData1.getIndexPageOffset() == timeSeriesChunkMetaData2
					.getIndexPageOffset());
			Utils.isListEqual(timeSeriesChunkMetaData1.getJsonMetaData(),
					timeSeriesChunkMetaData2.getJsonMetaData(), "json metadata");

			Utils.isTSeriesChunkMetadataEqual(timeSeriesChunkMetaData1.getTInTimeSeriesChunkMetaData(),
					timeSeriesChunkMetaData2.getTInTimeSeriesChunkMetaData());
			Utils.isVSeriesChunkMetadataEqual(timeSeriesChunkMetaData1.getVInTimeSeriesChunkMetaData(),
					timeSeriesChunkMetaData2.getVInTimeSeriesChunkMetaData());
		}
	}

	public static void isRowGroupMetaDataEqual(RowGroupMetaData rowGroupMetaData1,
											   RowGroupMetaData rowGroupMetaData2) {
		if (Utils.isTwoObjectsNotNULL(rowGroupMetaData1, rowGroupMetaData2, "RowGroupMetaData")) {
			assertTrue(rowGroupMetaData1.getDeltaObjectID().equals(rowGroupMetaData2.getDeltaObjectID()));
			assertTrue(
					rowGroupMetaData1.getDeltaObjectType().equals(rowGroupMetaData2.getDeltaObjectType()));
			assertTrue(rowGroupMetaData1.getTotalByteSize() == rowGroupMetaData2.getTotalByteSize());
			assertTrue(rowGroupMetaData1.getNumOfRows() == rowGroupMetaData2.getNumOfRows());

			if (Utils.isTwoObjectsNotNULL(rowGroupMetaData1.getPath(), rowGroupMetaData2.getPath(),
					"Row group metadata file path")) {
				assertTrue(rowGroupMetaData1.getPath().equals(rowGroupMetaData2.getPath()));
			}

			if (Utils.isTwoObjectsNotNULL(rowGroupMetaData1.getMetaDatas(),
					rowGroupMetaData2.getMetaDatas(), "TimeSeriesChunkMetaData List")) {
				List<TimeSeriesChunkMetaData> list1 = rowGroupMetaData1.getMetaDatas();
				List<TimeSeriesChunkMetaData> list2 = rowGroupMetaData2.getMetaDatas();

				if (list1.size() != list2.size()) {
					fail("TimeSeriesGroupMetaData List size is different");
				}

				for (int i = 0; i < list1.size(); i++) {
					Utils.isTimeSeriesChunkMetaDataEqual(list1.get(i), list2.get(i));
				}
			}
		}
	}

	public static void isRowGroupBlockMetadataEqual(TsRowGroupBlockMetaData rowGroupBlockMetaData1,
													TsRowGroupBlockMetaData rowGroupBlockMetaData2) {
		if (Utils.isTwoObjectsNotNULL(rowGroupBlockMetaData1, rowGroupBlockMetaData2,
				"RowGroupBlockMetaData")) {
			if (Utils.isTwoObjectsNotNULL(rowGroupBlockMetaData1.getRowGroups(),
					rowGroupBlockMetaData2.getRowGroups(), "Row Group List")) {
				List<RowGroupMetaData> list1 = rowGroupBlockMetaData1.getRowGroups();
				List<RowGroupMetaData> list2 = rowGroupBlockMetaData2.getRowGroups();
				if (list1.size() != list2.size()) {
					fail("TimeSeriesGroupMetaData List size is different");
				}
				// long maxNumRows = 0;
				for (int i = 0; i < list1.size(); i++) {
					Utils.isRowGroupMetaDataEqual(list1.get(i), list2.get(i));
					// maxNumRows += listTSF.get(i).getNumOfRows();
				}
				Utils.isStringSame(rowGroupBlockMetaData1.getDeltaObjectID(), rowGroupBlockMetaData2.getDeltaObjectID(), "delta object id");
			}
		}
	}

	public static void isFileMetaDataEqual(TsFileMetaData tsFileMetaData1, TsFileMetaData tsFileMetaData2) {
		if (Utils.isTwoObjectsNotNULL(tsFileMetaData1, tsFileMetaData2, "File MetaData")) {
			assertEquals(tsFileMetaData1.getCurrentVersion(), tsFileMetaData2.getCurrentVersion());
			assertEquals(tsFileMetaData1.getCreatedBy(), tsFileMetaData2.getCreatedBy());
			Utils.isTimeSeriesListEqual(tsFileMetaData1.getTimeSeriesList(), tsFileMetaData2.getTimeSeriesList(), 1);
			Utils.isListEqual(tsFileMetaData1.getJsonMetaData(), tsFileMetaData2.getJsonMetaData(), "json metadata");
			if (Utils.isTwoObjectsNotNULL(tsFileMetaData1.getProps(), tsFileMetaData2.getProps(), "user specified properties")) {
				Utils.isMapStringEqual(tsFileMetaData1.getProps(), tsFileMetaData2.getProps(), "Filemetadata properties");
			}
			if(Utils.isTwoObjectsNotNULL(tsFileMetaData1.getDeltaObjectMap(), tsFileMetaData2.getDeltaObjectMap(), "delta object map")) {
				Map<String, TsDeltaObject> map1 = tsFileMetaData1.getDeltaObjectMap();
				Map<String, TsDeltaObject> map2 = tsFileMetaData2.getDeltaObjectMap();
				if(map1.size() == map2.size()) {
					for(String key: map1.keySet()) {
						if(map1.containsKey(key)) {
							isDeltaObjectEqual(map1.get(key), map2.get(key));
						} else {
							fail(String.format("delta object map in thrift does not contain key %s", key));
						}
					}
				} else {
					fail(String.format("%s size is different", "delta object map"));
				}
			}
		}
	}

	public static void isDataPageHeaderEqual(DataPageHeader dataPageHeader1, DataPageHeader dataPageHeader2){
		if (Utils.isTwoObjectsNotNULL(dataPageHeader1, dataPageHeader2, "Data Page Header")) {
			assertEquals(dataPageHeader1.num_values, dataPageHeader2.num_values);
			assertEquals(dataPageHeader1.num_rows, dataPageHeader2.num_rows);
			if(Utils.isTwoObjectsNotNULL(dataPageHeader1.encoding, dataPageHeader2.encoding, "DataPageHeader TSEncoding")){
				assertEquals(dataPageHeader1.encoding.toString(), dataPageHeader2.encoding.toString());
			}
			if(Utils.isTwoObjectsNotNULL(dataPageHeader1.digest, dataPageHeader2.digest, "DataPageHeader Digest")){
				if(Utils.isTwoObjectsNotNULL(dataPageHeader1.digest.getStatistics(), dataPageHeader2.digest.getStatistics(), "statistics")) {
					Map<String, ByteBuffer> map1 = dataPageHeader1.digest.getStatistics();
					Map<String, ByteBuffer> map2 = dataPageHeader2.digest.getStatistics();
					if(map1.size() == map2.size()) {
						for(String key: map1.keySet()) {
							if(map1.containsKey(key)) {
								byte[] bytes1 = map1.get(key).array();
								byte[] bytes2 = map2.get(key).array();
								if(Utils.isTwoObjectsNotNULL(bytes1, bytes2, "Byte Buffer")){
									assertEquals(bytes1.length, bytes2.length);
									for(int i = 0;i < bytes1.length;i++){
										assertEquals(bytes1[i], bytes2[i]);
									}
								}
							} else {
								fail(String.format("statistics in digest2 does not contain key %s", key));
							}
						}
					} else {
						fail(String.format("%s size is different", "statistics"));
					}
				}
			}
			assertEquals(dataPageHeader1.is_compressed, dataPageHeader2.is_compressed);
			assertEquals(dataPageHeader1.max_timestamp, dataPageHeader2.max_timestamp);
			assertEquals(dataPageHeader1.min_timestamp, dataPageHeader2.min_timestamp);
		}
	}

	public static void isPageHeaderEqual(PageHeader pageHeader1, PageHeader pageHeader2) {
		if (Utils.isTwoObjectsNotNULL(pageHeader1, pageHeader2, "Page Header")) {
			if(Utils.isTwoObjectsNotNULL(pageHeader1.type, pageHeader2.type, "Page type")){
				assertEquals(pageHeader1.type.toString(), pageHeader2.type.toString());
			}
			assertEquals(pageHeader1.uncompressed_page_size, pageHeader2.uncompressed_page_size);
			assertEquals(pageHeader1.compressed_page_size, pageHeader2.compressed_page_size);
			assertEquals(pageHeader1.crc, pageHeader2.crc);
			if(Utils.isTwoObjectsNotNULL(pageHeader1.data_page_header, pageHeader2.data_page_header, "Data Page Header")){
				isDataPageHeaderEqual(pageHeader1.data_page_header, pageHeader2.data_page_header);
			}
			if(Utils.isTwoObjectsNotNULL(pageHeader1.index_page_header, pageHeader2.index_page_header, "Index Page Header")){
				assertEquals(pageHeader1.index_page_header, pageHeader2.index_page_header);
			}
			if(Utils.isTwoObjectsNotNULL(pageHeader1.dictionary_page_header, pageHeader2.dictionary_page_header, "Dictionary Page Header")){
				assertEquals(pageHeader1.dictionary_page_header, pageHeader2.dictionary_page_header);
			}
		}
	}
}
