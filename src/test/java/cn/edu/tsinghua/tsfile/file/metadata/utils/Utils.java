package cn.edu.tsinghua.tsfile.file.metadata.utils;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.format.TimeInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.format.TimeSeries;
import cn.edu.tsinghua.tsfile.format.ValueInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.format.DeltaObject;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

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

	public static void isTimeSeriesEqual(TimeSeriesMetadata timeSeriesInTSF, TimeSeries timeSeriesInThrift) {
		if (Utils.isTwoObjectsNotNULL(timeSeriesInTSF.getMeasurementUID(), timeSeriesInThrift.getMeasurement_uid(),
				"sensorUID")) {
			assertTrue(timeSeriesInTSF.getMeasurementUID().equals(timeSeriesInThrift.getMeasurement_uid()));
		}
		assertTrue(timeSeriesInTSF.getTypeLength() == timeSeriesInThrift.getType_length());
		if (Utils.isTwoObjectsNotNULL(timeSeriesInTSF.getType(), timeSeriesInThrift.getType(), "data type")) {
			assertTrue(timeSeriesInTSF.getType().toString() == timeSeriesInThrift.getType().toString());
		}
		if (Utils.isTwoObjectsNotNULL(timeSeriesInTSF.getFreqType(), timeSeriesInThrift.getFreq_type(), "freq type")) {
			assertTrue(timeSeriesInTSF.getFreqType().toString() == timeSeriesInThrift.getFreq_type().toString());
		}

		Utils.isListEqual(timeSeriesInTSF.getFrequencies(), timeSeriesInThrift.getFrequencies(), "frequencies");
		Utils.isListEqual(timeSeriesInTSF.getEnumValues(), timeSeriesInThrift.getEnum_values(), "data values");
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

	public static void isTimeSeriesListEqual(List<TimeSeriesMetadata> timeSeriesInTSF,
			List<TimeSeries> timeSeriesInThrift) {
		if (timeSeriesInTSF == null && timeSeriesInThrift == null)
			return;

		if (timeSeriesInTSF == null && timeSeriesInThrift == null)
			return;
		if ((timeSeriesInTSF == null) ^ (timeSeriesInThrift == null))
			fail("one list is null");
		if (timeSeriesInThrift.size() != timeSeriesInTSF.size())
			fail("list size is different");
		for (int i = 0; i < timeSeriesInThrift.size(); i++) {
			isTimeSeriesEqual(timeSeriesInTSF.get(i), timeSeriesInThrift.get(i));
		}
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

	public static void isTSeriesChunkMetadataEqual(TInTimeSeriesChunkMetaData tSeriesMetaData,
			TimeInTimeSeriesChunkMetaData timeInTimeSeriesChunkMetaData) {
		if (Utils.isTwoObjectsNotNULL(tSeriesMetaData, timeInTimeSeriesChunkMetaData,
				"TimeInTimeSeriesChunkMetaData")) {
			Utils.isStringSame(tSeriesMetaData.getDataType(), timeInTimeSeriesChunkMetaData.getData_type(),
					"data type");
			Utils.isStringSame(tSeriesMetaData.getFreqType(), timeInTimeSeriesChunkMetaData.getFreq_type(),
					"freq type");
			assertTrue(tSeriesMetaData.getStartTime() == timeInTimeSeriesChunkMetaData.getStartime());
			assertTrue(tSeriesMetaData.getEndTime() == timeInTimeSeriesChunkMetaData.getEndtime());
			Utils.isListEqual(tSeriesMetaData.getFrequencies(), timeInTimeSeriesChunkMetaData.getFrequencies(),
					"frequencies");
			Utils.isListEqual(tSeriesMetaData.getEnumValues(), timeInTimeSeriesChunkMetaData.getEnum_values(),
					"data values");
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

	public static void isDeltaObjectEqual(TsDeltaObject deltaObjectInTSF, DeltaObject deltaObjectInTHrift) {
		if (Utils.isTwoObjectsNotNULL(deltaObjectInTSF, deltaObjectInTHrift, "Delta object")) {
			assertTrue(deltaObjectInTSF.offset == deltaObjectInTHrift.getOffset());
			assertTrue(deltaObjectInTSF.metadataBlockSize == deltaObjectInTHrift.getMetadata_block_size());
			assertTrue(deltaObjectInTSF.startTime == deltaObjectInTHrift.getStart_time());
			assertTrue(deltaObjectInTSF.endTime == deltaObjectInTHrift.getEnd_time());
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

	public static void isVSeriesChunkMetadataEqual(VInTimeSeriesChunkMetaData vSeriesMetaData,
													ValueInTimeSeriesChunkMetaData valueInTimeSeriesChunkMetaData) {
		if (Utils.isTwoObjectsNotNULL(vSeriesMetaData, valueInTimeSeriesChunkMetaData,
				"ValueInTimeSeriesChunkMetaData")) {
			assertTrue(vSeriesMetaData.getMaxError() == valueInTimeSeriesChunkMetaData.getMax_error());
			assertTrue(vSeriesMetaData.getDataType().toString()
					.equals(valueInTimeSeriesChunkMetaData.getData_type().toString()));
			if (Utils.isTwoObjectsNotNULL(vSeriesMetaData.getDigest(), valueInTimeSeriesChunkMetaData.getDigest(),
					"Digest")) {
				Utils.isMapBufferEqual(vSeriesMetaData.getDigest().getStatistics(),
						valueInTimeSeriesChunkMetaData.getDigest().getStatistics(),
						"Diges statistics map");
			}
			Utils.isListEqual(vSeriesMetaData.getEnumValues(), valueInTimeSeriesChunkMetaData.getEnum_values(),
					"data values");
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

	public static void isTimeSeriesChunkMetaDataEqual(TimeSeriesChunkMetaData timeSeriesChunkMetaDataInTSF,
			cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData timeSeriesChunkMetaDataInThrift) {
		if (Utils.isTwoObjectsNotNULL(timeSeriesChunkMetaDataInTSF, timeSeriesChunkMetaDataInThrift,
				"TimeSeriesChunkMetaData")) {
			assertTrue(timeSeriesChunkMetaDataInTSF.getProperties().getMeasurementUID()
					.equals(timeSeriesChunkMetaDataInThrift.getMeasurement_uid()));
			assertTrue(timeSeriesChunkMetaDataInTSF.getProperties().getTsChunkType().toString()
					.equals(timeSeriesChunkMetaDataInThrift.getTimeseries_chunk_type().toString()));
			assertTrue(timeSeriesChunkMetaDataInTSF.getProperties().getFileOffset() == timeSeriesChunkMetaDataInThrift
					.getFile_offset());
			assertTrue(timeSeriesChunkMetaDataInTSF.getProperties().getCompression().toString()
					.equals(timeSeriesChunkMetaDataInThrift.getCompression_type().toString()));

			assertTrue(timeSeriesChunkMetaDataInTSF.getNumRows() == timeSeriesChunkMetaDataInThrift.getNum_rows());
			assertTrue(timeSeriesChunkMetaDataInTSF.getTotalByteSize() == timeSeriesChunkMetaDataInThrift
					.getTotal_byte_size());
			assertTrue(timeSeriesChunkMetaDataInTSF.getDataPageOffset() == timeSeriesChunkMetaDataInThrift
					.getData_page_offset());
			assertTrue(timeSeriesChunkMetaDataInTSF.getDictionaryPageOffset() == timeSeriesChunkMetaDataInThrift
					.getDictionary_page_offset());
			assertTrue(timeSeriesChunkMetaDataInTSF.getIndexPageOffset() == timeSeriesChunkMetaDataInThrift
					.getIndex_page_offset());
			Utils.isListEqual(timeSeriesChunkMetaDataInTSF.getJsonMetaData(),
					timeSeriesChunkMetaDataInThrift.getJson_metadata(), "json metadata");

			Utils.isTSeriesChunkMetadataEqual(timeSeriesChunkMetaDataInTSF.getTInTimeSeriesChunkMetaData(),
					timeSeriesChunkMetaDataInThrift.getTime_tsc());
			Utils.isVSeriesChunkMetadataEqual(timeSeriesChunkMetaDataInTSF.getVInTimeSeriesChunkMetaData(),
					timeSeriesChunkMetaDataInThrift.getValue_tsc());
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

	public static void isRowGroupMetaDataEqual(RowGroupMetaData rowGroupMetaDataInTSF,
			cn.edu.tsinghua.tsfile.format.RowGroupMetaData rowGroupMetaDataInThrift) {
		if (Utils.isTwoObjectsNotNULL(rowGroupMetaDataInTSF, rowGroupMetaDataInThrift, "RowGroupMetaData")) {
			assertTrue(rowGroupMetaDataInTSF.getDeltaObjectID().equals(rowGroupMetaDataInThrift.getDelta_object_id()));
			assertTrue(
					rowGroupMetaDataInTSF.getDeltaObjectType().equals(rowGroupMetaDataInThrift.getDelta_object_type()));
			assertTrue(rowGroupMetaDataInTSF.getTotalByteSize() == rowGroupMetaDataInThrift.getTotal_byte_size());
			assertTrue(rowGroupMetaDataInTSF.getNumOfRows() == rowGroupMetaDataInThrift.getMax_num_rows());

			if (Utils.isTwoObjectsNotNULL(rowGroupMetaDataInTSF.getPath(), rowGroupMetaDataInThrift.getFile_path(),
					"Row group metadata file path")) {
				assertTrue(rowGroupMetaDataInTSF.getPath().equals(rowGroupMetaDataInThrift.getFile_path()));
			}

			if (Utils.isTwoObjectsNotNULL(rowGroupMetaDataInTSF.getMetaDatas(),
					rowGroupMetaDataInThrift.getTsc_metadata(), "TimeSeriesChunkMetaData List")) {
				List<TimeSeriesChunkMetaData> listTSF = rowGroupMetaDataInTSF.getMetaDatas();
				List<cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData> listThrift = rowGroupMetaDataInThrift
						.getTsc_metadata();

				if (listTSF.size() != listThrift.size()) {
					fail("TimeSeriesGroupMetaData List size is different");
				}

				for (int i = 0; i < listTSF.size(); i++) {
					Utils.isTimeSeriesChunkMetaDataEqual(listTSF.get(i), listThrift.get(i));
				}
			}
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

	public static void isRowGroupBlockMetadataEqual(TsRowGroupBlockMetaData rowGroupBlockMetaDataInTSF,
			RowGroupBlockMetaData rowGroupBlockMetaDataInThrift) {
		if (Utils.isTwoObjectsNotNULL(rowGroupBlockMetaDataInTSF, rowGroupBlockMetaDataInThrift,
				"RowGroupBlockMetaData")) {
			if (Utils.isTwoObjectsNotNULL(rowGroupBlockMetaDataInTSF.getRowGroups(),
					rowGroupBlockMetaDataInThrift.getRow_groups_metadata(), "Row Group List")) {
				List<RowGroupMetaData> listTSF = rowGroupBlockMetaDataInTSF.getRowGroups();
				List<cn.edu.tsinghua.tsfile.format.RowGroupMetaData> listThrift = rowGroupBlockMetaDataInThrift
						.getRow_groups_metadata();
				if (listTSF.size() != listThrift.size()) {
					fail("TimeSeriesGroupMetaData List size is different");
				}
				// long maxNumRows = 0;
				for (int i = 0; i < listTSF.size(); i++) {
					Utils.isRowGroupMetaDataEqual(listTSF.get(i), listThrift.get(i));
					// maxNumRows += listTSF.get(i).getNumOfRows();
				}
				Utils.isStringSame(rowGroupBlockMetaDataInTSF.getDeltaObjectID(), rowGroupBlockMetaDataInThrift.getDelta_object_id(), "delta object id");
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

	public static void isFileMetaDataEqual(TsFileMetaData fileMetaDataInTSF, FileMetaData fileMetaDataInThrift) {
		if (Utils.isTwoObjectsNotNULL(fileMetaDataInTSF, fileMetaDataInThrift, "File MetaData")) {
			assertEquals(fileMetaDataInThrift.version, fileMetaDataInTSF.getCurrentVersion());
			assertEquals(fileMetaDataInThrift.getCreated_by(), fileMetaDataInTSF.getCreatedBy());
			Utils.isTimeSeriesListEqual(fileMetaDataInTSF.getTimeSeriesList(), fileMetaDataInThrift.getTimeseries_list());
			Utils.isListEqual(fileMetaDataInTSF.getJsonMetaData(), fileMetaDataInThrift.getJson_metadata(), "json metadata");
			if (Utils.isTwoObjectsNotNULL(fileMetaDataInTSF.getProps(), fileMetaDataInThrift.getProperties(), "user specified properties")) {
				Utils.isMapStringEqual(fileMetaDataInTSF.getProps(), fileMetaDataInThrift.getProperties(), "Filemetadata properties");
			}
			if(Utils.isTwoObjectsNotNULL(fileMetaDataInTSF.getDeltaObjectMap(), fileMetaDataInThrift.getDelta_object_map(), "delta object map")) {
				Map<String, TsDeltaObject> mapInTSF = fileMetaDataInTSF.getDeltaObjectMap();
				Map<String, DeltaObject> mapInThrift = fileMetaDataInThrift.getDelta_object_map();
				if(mapInThrift.size() == mapInTSF.size()) {
					for(String key: mapInTSF.keySet()) {
						if(mapInThrift.containsKey(key)) {
							isDeltaObjectEqual(mapInTSF.get(key), mapInThrift.get(key));
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
}
