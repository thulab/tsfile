package cn.edu.tsinghua.tsfile.file.metadata.utils;

import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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


	public static void isTimeSeriesChunkMetadataEqual(ChunkMetaData metadata1,
													  ChunkMetaData metadata2) {
		if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "ChunkMetaData")) {
			if (Utils.isTwoObjectsNotNULL(metadata1.getMeasurementUID(), metadata2.getMeasurementUID(),
					"sensorUID")) {
				assertTrue(metadata1.getMeasurementUID().equals(metadata2.getMeasurementUID()));
			}
			assertTrue(metadata1.getFileOffsetOfCorrespondingData() == metadata2.getFileOffsetOfCorrespondingData());
			assertTrue(metadata1.getNumOfPoints() == metadata2.getNumOfPoints());
			assertTrue(metadata1.getStartTime() == metadata2.getStartTime());
			assertTrue(metadata1.getEndTime() == metadata2.getEndTime());
			if (Utils.isTwoObjectsNotNULL(metadata1.getDigest(), metadata2.getDigest(), "digest")) {
				Utils.isMapBufferEqual(metadata1.getDigest().getStatistics(), metadata2.getDigest().getStatistics(), "statistics");
			}
		}
	}

	public static void isDeltaObjectEqual(TsDeviceMetadata metadata1, TsDeviceMetadata metadata2) {
		if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "DeltaObjectMetaData")) {
            assertEquals(metadata1.getStartTime(), metadata2.getStartTime());
            assertEquals(metadata1.getEndTime(), metadata2.getEndTime());

            if (Utils.isTwoObjectsNotNULL(metadata1.getRowGroups(), metadata2.getRowGroups(),
                    "Rowgroup metadata list")) {
                assertEquals(metadata1.getRowGroups().size(), metadata2.getRowGroups().size());
                for(int i = 0;i < metadata1.getRowGroups().size();i++){
                    Utils.isRowGroupMetaDataEqual(metadata1.getRowGroups().get(i),
                            metadata1.getRowGroups().get(i));
                }
            }
		}
	}

	public static void isRowGroupMetaDataEqual(ChunkGroupMetaData metadata1, ChunkGroupMetaData metadata2) {
		if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "ChunkGroupMetaData")) {
			assertTrue(metadata1.getDeltaObjectID().equals(metadata2.getDeltaObjectID()));

			if (Utils.isTwoObjectsNotNULL(metadata1.getChunkMetaDataList(), metadata2.getChunkMetaDataList(),
					"Timeseries chunk metadata list")) {
				assertEquals(metadata1.getChunkMetaDataList().size(),
                        metadata2.getChunkMetaDataList().size());
				for(int i = 0; i < metadata1.getChunkMetaDataList().size(); i++){
				    Utils.isTimeSeriesChunkMetadataEqual(metadata1.getChunkMetaDataList().get(i),
                            metadata1.getChunkMetaDataList().get(i));
                }
			}
		}
	}

	public static void isFileMetaDataEqual(TsFileMetaData metadata1, TsFileMetaData metadata2) {
		if (Utils.isTwoObjectsNotNULL(metadata1, metadata2, "File MetaData")) {
            if (Utils.isTwoObjectsNotNULL(metadata1.getDeltaObjectMap(), metadata2.getDeltaObjectMap(),
                    "Delta object metadata list")) {
                Map<String, TsDeviceMetadata> deviceMetadataMap1 = metadata1.getDeltaObjectMap();
                Map<String, TsDeviceMetadata> deviceMetadataMap2 = metadata2.getDeltaObjectMap();
                assertEquals(deviceMetadataMap1.size(), deviceMetadataMap2.size());
                Iterator<String> iterator = deviceMetadataMap1.keySet().iterator();
                while(iterator.hasNext()) {
                    String key = iterator.next();
                    assertTrue(deviceMetadataMap2.containsKey(key));
                    Utils.isDeltaObjectEqual(deviceMetadataMap1.get(key), deviceMetadataMap2.get(key));
                }
            }

            if (Utils.isTwoObjectsNotNULL(metadata1.getMeasurementSchema(), metadata2.getMeasurementSchema(),
                    "Timeseries metadata list")) {
                assertEquals(metadata1.getMeasurementSchema().size(), metadata2.getMeasurementSchema().size());
                for(Map.Entry<String, MeasurementSchema> entry: metadata1.getMeasurementSchema().entrySet()){
                	entry.getValue().equals(metadata2.getMeasurementSchema().get(entry.getKey()));
                }
            }

			assertEquals(metadata1.getCurrentVersion(), metadata2.getCurrentVersion());
			assertEquals(metadata1.getCreatedBy(), metadata2.getCreatedBy());
		}
	}
}
