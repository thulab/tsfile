package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.format.Digest;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * For more information, see Digest in cn.edu.thu.tsfile.format package
 */
public class TsDigest implements IConverter<Digest> {
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

	@Override
	public Digest convertToThrift() {
		Digest digest = new Digest();
		if (statistics != null) {
			Map<String, ByteBuffer> statisticsInThrift = new HashMap<>();
			for (String key : statistics.keySet()) {
				statisticsInThrift.put(key, statistics.get(key));
			}
			digest.setStatistics(statisticsInThrift);
		}
		return digest;
	}

	@Override
	public void convertToTSF(Digest digestInThrift) {
		if (digestInThrift != null) {
			Map<String, ByteBuffer> statisticsInThrift = digestInThrift.getStatistics();
			if (statisticsInThrift != null) {
				statistics = new HashMap<>();
				for (String key : statisticsInThrift.keySet()) {
					statistics.put(key, byteBufferDeepCopy(statisticsInThrift.get(key)));
				}
			} else {
				statistics = null;
			}
		}
	}

	public void write(OutputStream outputStream) throws IOException {
		ReadWriteToBytesUtils.writeIsNull(statistics, outputStream);

		if(statistics != null) {
			outputStream.write(statistics.size());
			for (Map.Entry<String, ByteBuffer> entry : statistics.entrySet()) {
				ReadWriteToBytesUtils.write(entry.getKey(), outputStream);
				ReadWriteToBytesUtils.write(entry.getValue(), outputStream);
			}
		}
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
