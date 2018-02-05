package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.IBytesConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * For more information, see Digest in cn.edu.thu.tsfile.format package
 */
public class TsDigest implements IBytesConverter {
	/**
	 * Digest/statistics per row group and per page.
	 */
	public Map<String, ByteBuffer> statistics;

	public TsDigest() {
	}

	public TsDigest(Map<String, ByteBuffer> statistics) {
		this.statistics = statistics;
	}
	
	public void setStatistics(Map<String,ByteBuffer> statistics) {
		this.statistics = statistics;
	}
	
	public Map<String, ByteBuffer> getStatistics(){
		return this.statistics;
	}
	
	public void addStatistics(String key, ByteBuffer value) {
		if(statistics == null) {
			statistics = new HashMap<>();
		}
		statistics.put(key, value);
	}

	@Override
	public String toString() {
		return statistics != null ? statistics.toString() : "";
	}

	public int write(OutputStream outputStream) throws IOException {
		int byteLen = 0;

		byteLen += ReadWriteToBytesUtils.writeIsNull(statistics, outputStream);
		if(statistics != null) {
			byteLen += ReadWriteToBytesUtils.write(statistics.size(), outputStream);
			for (Map.Entry<String, ByteBuffer> entry : statistics.entrySet()) {
				byteLen += ReadWriteToBytesUtils.write(entry.getKey(), outputStream);
				byteLen += ReadWriteToBytesUtils.write(entry.getValue(), outputStream);
			}
		}

		return byteLen;
	}

	public void read(InputStream inputStream) throws IOException {
		if(ReadWriteToBytesUtils.readIsNull(inputStream)) {
			statistics = new HashMap<>();
			int size = ReadWriteToBytesUtils.readInt(inputStream);

			String key;
			ByteBuffer value;
			for (int i = 0; i < size; i++) {
				key = ReadWriteToBytesUtils.readString(inputStream);
				value = ReadWriteToBytesUtils.readByteBuffer(inputStream);

				statistics.put(key, value);
			}
		}
	}

	public ByteBuffer byteBufferDeepCopy(ByteBuffer src) {
		ByteBuffer copy = ByteBuffer.allocate(src.remaining()).put(src.slice());
		copy.flip();
		return copy;
	}
}
