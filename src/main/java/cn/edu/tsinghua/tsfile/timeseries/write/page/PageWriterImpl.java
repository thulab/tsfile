package cn.edu.tsinghua.tsfile.timeseries.write.page;

import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

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
    private ListByteArrayOutputStream buf;
    private long totalValueCount;
    private long maxTimestamp;
    private long minTimestamp = -1;

    public PageWriterImpl(MeasurementDescriptor desc) {
        this.desc = desc;
        this.compressor = desc.getCompressor();
        this.buf = new ListByteArrayOutputStream();
    }

    @Override
    public void writePage(ListByteArrayOutputStream listByteArray, int valueCount, Statistics<?> statistics,
                          long maxTimestamp, long minTimestamp) throws PageException {
        // compress the input data
        if (this.minTimestamp == -1)
            this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        int uncompressedSize = listByteArray.size();
        ListByteArrayOutputStream compressedBytes = compressor.compress(listByteArray);
        int compressedSize = compressedBytes.size();
        PublicBAOS tempOutputStream = new PublicBAOS(estimateMaxPageHeaderSize() + compressedSize);
        // write the page header to IOWriter
        try {
            ReadWriteToBytesUtils.writeDataPageHeader(uncompressedSize, compressedSize, valueCount, statistics,
                    valueCount, desc.getEncodingType(), maxTimestamp, minTimestamp, tempOutputStream);
        } catch (IOException e) {
            resetTimeStamp();
            throw new PageException(
                    "meet IO Exception in writeDataPageHeader,ignore this page,error message:" + e.getMessage());
        }
        this.totalValueCount += valueCount;
        try {
            compressedBytes.writeAllTo(tempOutputStream);
        } catch (IOException e) {
            /*
			 * In our method, this line is to flush listByteArray to buf, both
			 * of them are in class of ListByteArrayOutputStream which contain
			 * several ByteArrayOutputStream. In general, they won't throw
			 * exception. The IOException is just for interface requirement of
			 * OutputStream.
			 */
            throw new PageException("meet IO Exception in buffer append,but we cannot understand it:" + e.getMessage());
        }
        buf.append(tempOutputStream);
        LOG.debug("page {}:write page from seriesWriter, valueCount:{}, stats:{},size:{}", desc, valueCount, statistics,
                estimateMaxPageMemSize());
    }

    public Pair<List<ByteArrayInputStream>, CompressionTypeName> query() {


        List<ByteArrayInputStream> backupPageList = buf.transform();
        Pair<List<ByteArrayInputStream>, CompressionTypeName> ret = new Pair<List<ByteArrayInputStream>, CompressionTypeName>(
                backupPageList, compressor.getCodecName());
        return ret;
    }

    private void resetTimeStamp() {
        if (totalValueCount == 0)
            minTimestamp = -1;
    }

    @Override
    public void writeToFileWriter(TsFileIOWriter writer, Statistics<?> statistics) throws IOException {
        writer.startSeries(desc, compressor.getCodecName(), desc.getType(), statistics, maxTimestamp, minTimestamp);
        long totalByteSize = writer.getPos();
        writer.writeBytesToStream(buf);
        LOG.debug("write series to file finished:{}", desc);
        long size = writer.getPos() - totalByteSize;
        writer.endSeries(size, totalValueCount);
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

    public int calculatePageHeaderSize(int digestSize) {
        //PageHeader: PageType--4, uncompressedSize--4,compressedSize--4
        //DatapageHeader: numValues--4, numNulls--4, numRows--4, Encoding--4, isCompressed--1, maxTimestamp--8, minTimestamp--8
        //Digest: max ByteBuffer, min ByteBuffer
        // * 2 to caculate max object size in memory

        return 2 * (45 + digestSize);
    }
}
