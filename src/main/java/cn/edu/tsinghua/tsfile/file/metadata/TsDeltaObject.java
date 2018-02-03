package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.IBytesConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TsDeltaObject implements IBytesConverter {
	/** start position of RowGroupMetadataBlock in file **/
	public long offset;

	/** size of RowGroupMetadataBlock in byte **/
	public int metadataBlockSize;

	/** start time for a delta object **/
	public long startTime;

	/** end time for a delta object **/
	public long endTime;

	public TsDeltaObject(){ }
	
	public TsDeltaObject(long offset, int metadataBlockSize, long startTime, long endTime){
		this.offset = offset;
		this.metadataBlockSize = metadataBlockSize;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public int write(OutputStream outputStream) throws IOException {
		int byteLen = 0;

		byteLen += ReadWriteToBytesUtils.write(offset, outputStream);
		byteLen += ReadWriteToBytesUtils.write(metadataBlockSize, outputStream);
		byteLen += ReadWriteToBytesUtils.write(startTime, outputStream);
		byteLen += ReadWriteToBytesUtils.write(endTime, outputStream);

		return byteLen;
	}

	public void read(InputStream inputStream) throws IOException {
		offset = ReadWriteToBytesUtils.readLong(inputStream);
		metadataBlockSize = ReadWriteToBytesUtils.readInt(inputStream);
		startTime = ReadWriteToBytesUtils.readLong(inputStream);
		endTime = ReadWriteToBytesUtils.readLong(inputStream);
	}
}
