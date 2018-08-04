package cn.edu.tsinghua.tsfile.timeseries.write.page;

import cn.edu.tsinghua.tsfile.common.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * a implementation of {@linkplain IPageWriter IPageWriter}
 *
 * @author kangrong
 * @see IPageWriter IPageWriter
 */
public class PageWriterImpl implements IPageWriter {
    private static Logger LOG = LoggerFactory.getLogger(PageWriterImpl.class);
    private final Compressor compressor;
    private final MeasurementDescriptor desc;
    private PublicBAOS buf;
    private long totalValueCount;
    private long maxTimestamp;
    private long minTimestamp = -1;
    private ByteBuffer compressedData;//DirectByteBuffer

    public PageWriterImpl(MeasurementDescriptor desc) {
        this.desc = desc;
        this.compressor = desc.getCompressor();
        this.buf = new PublicBAOS();
    }


    /**
     * write the page header and data into the PageWriter's outputstream
     * @param data the data of the page
     * @param valueCount    - the amount of values in that page
     * @param statistics    - the statistics for that page
     * @param maxTimestamp  - timestamp maximum in given data
     * @param minTimestamp  - timestamp minimum in given data
     * @return  byte size of the page header and uncompressed data in the page body.
     * @throws PageException
     */
    @Override
    public int writePageHeaderAndDataIntoBuff(ByteBuffer data, int valueCount, Statistics<?> statistics,
                                              long maxTimestamp, long minTimestamp) throws PageException {
        // compress the input data
        if (this.minTimestamp == -1)
            this.minTimestamp = minTimestamp;
        if(this.minTimestamp==-1){
        	LOG.error("Write page error, {}, minTime:{}, maxTime:{}",desc,minTimestamp,maxTimestamp);
        }
        this.maxTimestamp = maxTimestamp;
        int uncompressedSize = data.remaining();
        int maxSize=compressor.getMaxBytesForCompression(uncompressedSize);
        int compressedSize=0;
        int compressedPosition=0;
        byte[] compressedBytes = null;

        if(compressor.getCodecName().equals(CompressionType.UNCOMPRESSED)) {
            compressedSize=data.remaining();
        }else{
            if (data.isDirect()) {
                if (compressedData == null || compressedData.remaining() < maxSize) {
                    if (compressedData != null) {
                        ((DirectBuffer) compressedData).cleaner().clean();
                    }
                    compressedData = ByteBuffer.allocateDirect(maxSize);
                }
                try {
                    compressedSize = compressor.compress(data, compressedData);
                } catch (IOException e) {
                    throw new PageException(
                            "Error when writing a page, " + e.getMessage());
                }
            } else {
                if (compressedBytes == null || compressedBytes.length < compressor.getMaxBytesForCompression(uncompressedSize)) {
                    compressedBytes = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
                }
                try {
                    compressedPosition = 0;
                    compressedSize = compressor.compress(data.array(), data.position(), data.remaining(), compressedBytes);
                } catch (IOException e) {
                    throw new PageException(
                            "Error when writing a page, " + e.getMessage());
                }
            }
        }

        int headerSize=0;
        //PublicBAOS tempOutputStream = new PublicBAOS(estimateMaxPageHeaderSize() + compressedSize);
        // write the page header to IOWriter
        try {
//            ReadWriteThriftFormatUtils.writeDataPageHeader(uncompressedSize, compressedSize, valueCount, statistics,
//                    valueCount, desc.getEncodingType(), tempOutputStream, maxTimestamp, minTimestamp);
            PageHeader header=new PageHeader(uncompressedSize, compressedSize, valueCount, statistics, maxTimestamp, minTimestamp);
            headerSize=header.getSerializedSize();
            LOG.debug("start to flush a page header into buffer, buffer position {} ", buf.size());
            header.serializeTo(buf);
            LOG.debug("finish to flush a page header {} of {} into buffer, buffer position {} ", header, desc.getMeasurementId(), buf.size());

        } catch (IOException e) {
            resetTimeStamp();
            throw new PageException(
                    "IO Exception in writeDataPageHeader,ignore this page,error message:" + e.getMessage());
        }
        this.totalValueCount += valueCount;
        try {
            LOG.debug("start to flush a page data into buffer, buffer position {} ", buf.size());
            if(compressor.getCodecName().equals(CompressionType.UNCOMPRESSED)){
                WritableByteChannel channel = Channels.newChannel(buf);
                channel.write(data);
            }else {
                if (data.isDirect()) {
                    WritableByteChannel channel = Channels.newChannel(buf);
                    channel.write(compressedData);
                } else {
                    buf.write(compressedBytes, compressedPosition, compressedSize);
                }
            }
            LOG.debug("start to flush a page data into buffer, buffer position {} ", buf.size());
        } catch (IOException e) {
            throw new PageException("meet IO Exception in buffer append,but we cannot understand it:" + e.getMessage());
        }
//        LOG.debug("page {}:write page from seriesWriter, valueCount:{}, stats:{},size:{}", desc, valueCount, statistics,
//                estimateMaxPageMemSize());
        return headerSize+uncompressedSize;
    }

    private void resetTimeStamp() {
        if (totalValueCount == 0)
            minTimestamp = -1;
    }


    @Override
    public long writeAllPagesOfSeriesToTsFile(TsFileIOWriter writer, Statistics<?> statistics, int numOfPages) throws IOException {
    	if(minTimestamp==-1){
    		LOG.error("Write page error, {}, minTime:{}, maxTime:{}",desc,minTimestamp,maxTimestamp);
    	}
        int headerSize=writer.startFlushChunk(desc, compressor.getCodecName(), desc.getType(), desc.getEncodingType(),statistics, maxTimestamp, minTimestamp, buf.size(), numOfPages);

        long totalByteSize = writer.getPos();
        LOG.debug("start writing pages of {} into file, position {}", desc.getMeasurementId(), writer.getPos());
        writer.writeBytesToStream(buf);
        LOG.debug("finish writing pages of {} into file, position {}", desc.getMeasurementId(), writer.getPos());

        long size = writer.getPos() - totalByteSize;
        assert  size == buf.size();

        writer.endChunk(size + headerSize, totalValueCount);
//        LOG.debug("page {}:write page to fileWriter,type:{},maxTime:{},minTime:{},nowPos:{},stats:{}",
//                desc.getMeasurementId(), desc.getType(), maxTimestamp, minTimestamp, writer.getPos(), statistics);
        return headerSize + size;
    }

    @Override
    public void reset() {
        minTimestamp = -1;
        buf.reset();
        totalValueCount = 0;
    }

    @Override
    public long estimateMaxPageMemSize() {
        // return size of buffer + page max size;
        return buf.size() + estimateMaxPageHeaderSize();
    }

    private int estimateMaxPageHeaderSize() {
        int digestSize = (totalValueCount == 0) ? 0 : desc.getTypeLength() * 2;
        return PageHeader.calculatePageHeaderSize(desc.getType());
        //return calculatePageHeaderSize(digestSize);
    }

//    private int calculatePageHeaderSize(int digestSize) {//FIXME 放到page Header中秋u
//        //PageHeader: PageType--4, uncompressedSize--4,compressedSize--4
//        //DatapageHeader: numValues--4, numNulls--4, numRows--4, Encoding--4, isCompressed--1, maxTimestamp--8, minTimestamp--8
//        //Digest: max ByteBuffer, min ByteBuffer
//        // * 2 to caculate max object size in memory
//
//        return 2 * (45 + digestSize);
//    }

    @Override
    public long getCurrentDataSize(){
        return buf.size();
    }

}
