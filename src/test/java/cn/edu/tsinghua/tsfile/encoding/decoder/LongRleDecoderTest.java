package cn.edu.tsinghua.tsfile.encoding.decoder;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.encoding.encoder.RleEncoder;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.encoding.encoder.LongRleEncoder;

public class LongRleDecoderTest {
	private List<Long> rleList;
	private List<Long> bpList;
	private List<Long> hybridList;
	private int rleBitWidth;
	private int bpBitWidth;
	private int hybridWidth;
	
	@Before
	public void setUp() throws Exception {		
		rleList = new ArrayList<Long>();
		int rleCount = 11;
		int rleNum = 38;
		long rleStart = 11;
		for(int i = 0; i < rleNum;i++){	
			for(int j = 0;j < rleCount;j++){
				rleList.add(rleStart);
			}
			for(int j = 0;j < rleCount;j++){
				rleList.add(rleStart-1);
			}
			rleCount += 2;
			rleStart *= -3;
		}
		rleBitWidth = ReadWriteStreamUtils.getLongMaxBitWidth(rleList);
		
		bpList = new ArrayList<Long>();
		int bpCount = 15;
		long bpStart = 11;
		for(int i = 0; i < bpCount;i++){
			bpStart *= 3;
			if(i % 2 == 1){
				bpList.add(bpStart*-1);
			}else{
				bpList.add(bpStart);
			}
		}
		bpBitWidth = ReadWriteStreamUtils.getLongMaxBitWidth(bpList);
		
		hybridList = new ArrayList<Long>();
		int hybridCount = 11;
		int hybridNum = 1000;
		long hybridStart = 20;
		
		for(int i = 0;i < hybridNum;i++){
			for(int j = 0;j < hybridCount;j++){
				hybridStart += 3;
				if(j % 2 == 1){
					hybridList.add(hybridStart*-1);
				}else{
					hybridList.add(hybridStart);
				}
			}
			for(int j = 0;j < hybridCount;j++){
				if(i % 2 == 1){
					hybridList.add(hybridStart*-1);
				}else{
					hybridList.add(hybridStart);
				}
			}
			hybridCount += 2;
		}
		
		hybridWidth = ReadWriteStreamUtils.getLongMaxBitWidth(hybridList);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRleReadLong() throws IOException{
		for(int i = 1;i < 2;i++){
			testLength(rleList,rleBitWidth,false,i);
		}
	}
	
	@Test
	public void testMaxRLERepeatNUM() throws IOException{
		List<Long> repeatList = new ArrayList<>();
		int rleCount = 17;
		int rleNum = 5;
		long rleStart = 11;
		for(int i = 0; i < rleNum;i++){	
			for(int j = 0;j < rleCount;j++){
				repeatList.add(rleStart);
			}
			for(int j = 0;j < rleCount;j++){
				repeatList.add(rleStart / 3);
			}
			rleCount *= 7;
			rleStart *= -3;
		}
		int bitWidth = ReadWriteStreamUtils.getLongMaxBitWidth(repeatList);
		for(int i = 1;i < 10;i++){
			testLength(repeatList,bitWidth,false,i);
		}
	}
	
	@Test
	public void testBitPackingReadLong() throws IOException{
		for(int i = 1;i < 10;i++){
			testLength(bpList,bpBitWidth,false,i);
		}
	}
	
	@Test
	public void testHybridReadLong() throws IOException{
		for(int i = 1;i < 10;i++){
			long start = System.currentTimeMillis();
			testLength(hybridList,hybridWidth,false,i);
			long end = System.currentTimeMillis();
			System.out.println(String.format("Turn %d use time %d ms",i, end-start));
		}
	}
	
	@Test 
	public void testBitPackingReadHeader() throws IOException{
		for(int i = 1;i < 505;i++){
			testBitPackedReadHeader(i);
		}
	}
	
	private void testBitPackedReadHeader(int num) throws IOException{
		List<Long> list = new ArrayList<Long>();
		
		for(long i = 0; i < num;i++){
			list.add(i);
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int bitWidth = ReadWriteStreamUtils.getLongMaxBitWidth(list);
		RleEncoder<Long> encoder = new LongRleEncoder(EndianType.LITTLE_ENDIAN);
		for(long value : list){
			encoder.encode(value, baos);
		}
		encoder.flush(baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ReadWriteStreamUtils.readUnsignedVarInt(bais);
		assertEquals(bitWidth, bais.read());
		int header = ReadWriteStreamUtils.readUnsignedVarInt(bais);	
		int group = header >> 1;
		assertEquals(group, (num+7)/8);
		int lastBitPackedNum = bais.read();
		if(num % 8 == 0){
			assertEquals(lastBitPackedNum,8);
		} else{
			assertEquals(lastBitPackedNum, num % 8);
		}
	}
	
	public void testLength(List<Long> list,int bitWidth,boolean isDebug,int repeatCount) throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		RleEncoder<Long> encoder = new LongRleEncoder(EndianType.LITTLE_ENDIAN);
		for(int i = 0;i < repeatCount;i++){
			for(long value : list){
				encoder.encode(value, baos);
			}
			encoder.flush(baos);
		}
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		RleDecoder decoder = new LongRleDecoder(EndianType.LITTLE_ENDIAN);
		for(int i = 0;i < repeatCount;i++){
			for(long value : list){
				long value_ = decoder.readLong(bais);
				if(isDebug){
					System.out.println(value_+"/"+value);
				}
				assertEquals(value, value_);
			}
		}
	}
}
