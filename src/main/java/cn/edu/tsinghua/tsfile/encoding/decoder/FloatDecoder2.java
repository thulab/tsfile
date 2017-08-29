package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public abstract class FloatDecoder2 extends Decoder {
	protected static final int EOF = -1;
	protected boolean flag;
	
	protected int leadingZeroNum, tailingZeroNum;
	protected boolean isEnd;
	// 8-bit buffer of bits to write out
	protected int buffer;
	// number of bits remaining in buffer
	protected int n;
	
	protected boolean nextFlag1;
	protected boolean nextFlag2;

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

	protected boolean isEmpty() {
        return buffer == EOF;
    }
	
	protected boolean readBit(InputStream in) throws IOException {
		if(n == 0 && !isEnd){
			fillBuffer(in);
		}
		if (isEmpty()) throw new IOException("Reading from empty input stream");
        n--;
        boolean bit = ((buffer >> n) & 1) == 1;
        return bit;
    }
	
	protected void fillBuffer(InputStream in) {
        try {
            buffer = in.read();
            n = 8;
        } catch (IOException e) {
            System.err.println("EOF");
            buffer = EOF;
            n = -1;
        }
    }
    
	protected void checkNextFlags(InputStream in) throws IOException{
    		nextFlag1 = readBit(in);
    		nextFlag2 = readBit(in);
    		if(!nextFlag1 && nextFlag2){
    			isEnd = true;
    		}
    }
}
