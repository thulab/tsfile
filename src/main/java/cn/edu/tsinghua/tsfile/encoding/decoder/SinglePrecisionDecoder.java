package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public class SinglePrecisionDecoder extends FloatDecoder2 {

	private int preValue;

	public SinglePrecisionDecoder(TSEncoding type) {
		super(type);
	}

	@Override
	public float readFloat(InputStream in) {
		if (!flag) {
			flag = true;
			try {
				int ch1 = in.read();
				int ch2 = in.read();
				int ch3 = in.read();
				int ch4 = in.read();
				preValue = ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
				leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
				tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
				fillBuffer(in);
				checkNextFlags(in);
				return Float.intBitsToFloat(preValue);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				if (!nextFlag1 && !nextFlag2) {
					checkNextFlags(in);
					return Float.intBitsToFloat(preValue);
				}

				if (nextFlag1 && !nextFlag2) {
					int tmp = 0;
					for (int i = 0; i < TSFileConfig.FLOAT_LENGTH - leadingZeroNum - tailingZeroNum; i++) {
						int bit = readBit(in) ? 1 : 0;
						tmp |= bit << (TSFileConfig.FLOAT_LENGTH - 1 - leadingZeroNum - i);
					}
					tmp ^= preValue;
					checkNextFlags(in);
					return Float.intBitsToFloat(tmp);
				}

				if (nextFlag1 && nextFlag2) {
					int leadingZeroNumTmp = readIntFromStream(in, TSFileConfig.FLAOT_LEADING_ZERO_LENGTH);
					int lenTmp = readIntFromStream(in, TSFileConfig.FLOAT_VALUE_LENGTH);
					int tmp = readIntFromStream(in, lenTmp);
					tmp <<= (TSFileConfig.FLOAT_LENGTH - leadingZeroNumTmp - lenTmp);
					tmp ^= preValue;
					checkNextFlags(in);
					return Float.intBitsToFloat(tmp);
				}
			} catch (IOException e) {
				// TODO: handle exception
				e.printStackTrace();
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
}
