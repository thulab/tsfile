package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public class FloatDecoder2 extends Decoder {
	private static final int EOF = -1;
	private boolean flag;
	private int preValue;
	private int leadingZeroNum, tailingZeroNum;
	private boolean isEnd;
	// 8-bit buffer of bits to write out
	private int buffer;
	// number of bits remaining in buffer
	private int n;
	
	private boolean nextFlag1;
	private boolean nextFlag2;

	public FloatDecoder2(TSEncoding type) {
		super(type);
		this.flag = false;
		this.isEnd = false;
	}

	@Override
	public boolean hasNext(InputStream in) throws IOException {
		// TODO Auto-generated method stub
		if (in.available() > 0 || !isEnd) {
			return true;
		}
		return false;
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
        			checkNextFlags(in);
				return Float.intBitsToFloat(preValue);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else{
			try {
				if(!nextFlag1 && !nextFlag2){
					checkNextFlags(in);
					return Float.intBitsToFloat(preValue);
				}
				
				if(nextFlag1 && !nextFlag2){
					int tmp = 0;
					for(int i = 0; i < 32 - leadingZeroNum - tailingZeroNum; i++){
						int bit = readBit(in) ? 1 : 0;
						tmp |= bit << (31 - leadingZeroNum - i);
					}
					tmp ^= preValue; 
					checkNextFlags(in);
					return Float.intBitsToFloat(tmp);
				}
				
				if(nextFlag1 && nextFlag2){
					int leadingZeroNumTmp = 0;
					for(int i = 0; i < 5;i++){
						int bit = readBit(in) ? 1 : 0;
						leadingZeroNumTmp |= bit << (4 - i);
					}
					
					int lenTmp = 0;
					for(int i = 0; i < 6;i++){
						int bit = readBit(in) ? 1 : 0;
						lenTmp |= bit << (5 - i);
					}
					
					int tmp = 0;
					for(int i = 0; i < lenTmp;i++){
						int bit = readBit(in) ? 1 : 0;
						tmp |= bit << (lenTmp - 1 - i);
					}
					
					tmp <<= (32 - leadingZeroNumTmp - lenTmp);
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
	
	private boolean isEmpty() {
        return buffer == EOF;
    }
	
	private boolean readBit(InputStream in) throws IOException {
        if (isEmpty()) throw new IOException("Reading from empty input stream");
        n--;
        boolean bit = ((buffer >> n) & 1) == 1;
        if (n == 0) fillBuffer(in);
        return bit;
    }
	
    private void fillBuffer(InputStream in) {
        try {
            buffer = in.read();
            n = 8;
        } catch (IOException e) {
            System.err.println("EOF");
            buffer = EOF;
            n = -1;
        }
    }
    
    private void checkNextFlags(InputStream in) throws IOException{
    		nextFlag1 = readBit(in);
    		nextFlag2 = readBit(in);
    		if(!nextFlag1 && nextFlag2){
    			isEnd = true;
    		}
    }
}
