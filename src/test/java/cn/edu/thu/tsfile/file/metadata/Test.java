package cn.edu.thu.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.file.metadata.TSFileMetaData;
import cn.edu.thu.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.thu.tsfile.file.metadata.utils.TestHelper;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.format.FileMetaData;

public class Test {
	private static int deviceNum = 15000;
	private static int sensorNum = 10;
	private static String PATH = "test.ksn";
	
	public static RowGroupMetaData createSimpleRowGroupMetaDataInTSF() throws UnsupportedEncodingException {
		RowGroupMetaData metaData = new RowGroupMetaData(RowGroupMetaDataTest.DELTA_OBJECT_UID,
				RowGroupMetaDataTest.MAX_NUM_ROWS, RowGroupMetaDataTest.TOTAL_BYTE_SIZE, new ArrayList<>(),
				RowGroupMetaDataTest.DELTA_OBJECT_TYPE);
		metaData.setPath(RowGroupMetaDataTest.FILE_PATH);
		for(int i = 0; i < sensorNum;i++){
			metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF());
		}
		return metaData;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		TSFileMetaDataConverter converter = new TSFileMetaDataConverter();
		long startTime = System.currentTimeMillis();
		List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
		for (int i = 0; i < deviceNum; i++) {
			rowGroupMetaDatas.add(createSimpleRowGroupMetaDataInTSF());
		}
		TSFileMetaData tsFileMetaData = new TSFileMetaData(rowGroupMetaDatas, new ArrayList<>(), 1);
		System.out.println("1: "+(System.currentTimeMillis()-startTime));
		
		startTime = System.currentTimeMillis();
		FileMetaData fileMetaData = converter.toThriftFileMetadata(tsFileMetaData);
		System.out.println("2: "+(System.currentTimeMillis()-startTime));
		
		startTime = System.currentTimeMillis();
	    File file = new File(PATH);
	    if (file.exists())
	      file.delete();
	    FileOutputStream fos = new FileOutputStream(file);
	    RandomAccessOutputStream out = new RandomAccessOutputStream(file, "rw");
	    ReadWriteThriftFormatUtils.writeFileMetaData(fileMetaData, out.getOutputStream());

	    out.close();
	    fos.close();
	    
	    System.out.println("3: "+(System.currentTimeMillis()-startTime));
	    FileInputStream fis = new FileInputStream(file);
	    System.out.println("file size: "+fis.available());
	    fis.close();
	    
	    if (file.exists())
	      file.delete();
	}

}
