package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

public class DoublePrecisionDecoder extends GorillaDecoder{
	private static final Logger LOGGER = LoggerFactory.getLogger(DoublePrecisionDecoder.class);
	private long preValue;
	
	public DoublePrecisionDecoder() {
	}
	
	@Override
	public double readDouble(InputStream in) {
		if (!flag) {
			flag = true;
			try {
		        int[] buf = new int[8];
		        for (int i = 0; i < 8; i++)
	                buf[i] = in.read();
		        long res = 0L;
		        for (int i = 0; i < 8; i++) {
		            res += ((long) buf[i] << (i * 8));
		        }
		        preValue = res;
				leadingZeroNum = Long.numberOfLeadingZeros(preValue);
				tailingZeroNum = Long.numberOfTrailingZeros(preValue);
				fillBuffer(in);
				checkNextFlags(in);
				return Double.longBitsToDouble(preValue);
			} catch (IOException e) {
				LOGGER.error("DoublePrecisionDecoder cannot read first double number because: {}", e.getMessage());
			}
		} else {
			try {
				if (!nextFlag1 && !nextFlag2) {
					checkNextFlags(in);
					return Double.longBitsToDouble(preValue);
				}

				if (nextFlag1 && !nextFlag2) {
					long tmp = 0;
					for (int i = 0; i < TSFileConfig.DOUBLE_LENGTH - leadingZeroNum - tailingZeroNum; i++) {
						int bit = readBit(in) ? 1 : 0;
						tmp |= bit << (TSFileConfig.DOUBLE_LENGTH - 1 - leadingZeroNum - i);
					}
					tmp ^= preValue;
					checkNextFlags(in);
					return Double.longBitsToDouble(tmp);
				}

				if (nextFlag1 && nextFlag2) {
					int leadingZeroNumTmp = readIntFromStream(in, TSFileConfig.DOUBLE_LEADING_ZERO_LENGTH);
					int lenTmp = readIntFromStream(in, TSFileConfig.DOUBLE_VALUE_LENGTH);
					long tmp = readLongFromStream(in, lenTmp);
					tmp <<= (TSFileConfig.DOUBLE_LENGTH - leadingZeroNumTmp - lenTmp);
					tmp ^= preValue;
					checkNextFlags(in);
					return Double.longBitsToDouble(tmp);
				}
			} catch (IOException e) {
				LOGGER.error("DoublePrecisionDecoder cannot read following double number because: {}", e.getMessage());
			}
		}
		return Float.MIN_VALUE;
	}
	
	private int readIntFromStream(InputStream in, int end) throws IOException{
		int num = 0;
		for (int i = 0; i < end; i++) {
			int bit = readBit(in) ? 1 : 0;
			num |= bit << (end - 1 - i);
		}
		return num;
	}
	
	private long readLongFromStream(InputStream in, int end) throws IOException{
		long num = 0;
		for (int i = 0; i < end; i++) {
			long bit = (long)(readBit(in) ? 1 : 0);
			num |= bit << (end - 1 - i);
		}
		return num;
	}
}
