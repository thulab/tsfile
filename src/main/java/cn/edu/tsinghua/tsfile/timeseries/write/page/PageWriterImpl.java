package cn.edu.tsinghua.tsfile.timeseries.write.page;

import cn.edu.tsinghua.tsfile.common.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.file.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

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
     * @throws PageException
     */
    @Override
    public void writePageHeaderAndDataIntoBuff(ByteBuffer data, int valueCount, Statistics<?> statistics,
                                               long maxTimestamp, long minTimestamp) throws PageException {
        // compress the input data
        if (this.minTimestamp == -1)
            this.minTimestamp = minTimestamp;
        if(this.minTimestamp==-1){
        	LOG.error("Write page error, {}, minTime:{}, maxTime:{}",desc,minTimestamp,maxTimestamp);
        }
        this.maxTimestamp = maxTimestamp;
        int uncompressedSize = data.remaining();
        byte[] compressedBytes = null;
        try {
            compressedBytes = compressor.compress(data);
        } catch (IOException e) {
            throw new PageException(
                    "Error when writing a page, " + e.getMessage());
        }
        int compressedSize = compressedBytes.length;
        //PublicBAOS tempOutputStream = new PublicBAOS(estimateMaxPageHeaderSize() + compressedSize);
        // write the page header to IOWriter
        try {
//            ReadWriteThriftFormatUtils.writeDataPageHeader(uncompressedSize, compressedSize, valueCount, statistics,
//                    valueCount, desc.getEncodingType(), tempOutputStream, maxTimestamp, minTimestamp);
            new PageHeader(uncompressedSize, compressedSize, valueCount, statistics, maxTimestamp, minTimestamp)
                    .serializeTo(buf);
        } catch (IOException e) {
            resetTimeStamp();
            throw new PageException(
                    "IO Exception in writeDataPageHeader,ignore this page,error message:" + e.getMessage());
        }
        this.totalValueCount += valueCount;
        try {
            buf.write(compressedBytes);
        } catch (IOException e) {
            throw new PageException("meet IO Exception in buffer append,but we cannot understand it:" + e.getMessage());
        }
        LOG.debug("page {}:write page from seriesWriter, valueCount:{}, stats:{},size:{}", desc, valueCount, statistics,
                estimateMaxPageMemSize());
    }

    private void resetTimeStamp() {
        if (totalValueCount == 0)
            minTimestamp = -1;
    }

    @Override
    public void writeAllPagesOfSeriesToTsFile(TsFileIOWriter writer, Statistics<?> statistics) throws IOException {
    	if(minTimestamp==-1){
    		LOG.error("Write page error, {}, minTime:{}, maxTime:{}",desc,minTimestamp,maxTimestamp);
    	}
        writer.startChunk(desc, compressor.getCodecName(), desc.getType(), statistics, maxTimestamp, minTimestamp);
        long totalByteSize = writer.getPos();
        writer.writeBytesToStream(buf);
        LOG.debug("write series to file finished:{}", desc);
        long size = writer.getPos() - totalByteSize;
        writer.endChunk(size, totalValueCount);
        LOG.debug("page {}:write page to fileWriter,type:{},maxTime:{},minTime:{},nowPos:{},stats:{}",
                desc.getMeasurementId(), desc.getType(), maxTimestamp, minTimestamp, writer.getPos(), statistics);
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
        return calculatePageHeaderSize(digestSize);
    }

    private int calculatePageHeaderSize(int digestSize) {
        //PageHeader: PageType--4, uncompressedSize--4,compressedSize--4
        //DatapageHeader: numValues--4, numNulls--4, numRows--4, Encoding--4, isCompressed--1, maxTimestamp--8, minTimestamp--8
        //Digest: max ByteBuffer, min ByteBuffer
        // * 2 to caculate max object size in memory

        return 2 * (45 + digestSize);
    }
}
