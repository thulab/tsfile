package cn.edu.thu.tsfile.timeseries.write.page;

import java.io.IOException;

import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.common.utils.bytesinput.BytesInput;
import cn.edu.thu.tsfile.compress.Compressor;
import cn.edu.thu.tsfile.file.metadata.statistics.Statistics;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.utils.bytesinput.ListBytesInput;
import cn.edu.thu.tsfile.timeseries.write.exception.PageException;

/**
 * a implementation of {@linkplain IPageWriter IPageWriter}
 * 
 * @see IPageWriter IPageWriter
 * @author kangrong
 *
 */
public class PageWriterImpl implements IPageWriter {
    private static Logger LOG = LoggerFactory.getLogger(PageWriterImpl.class);

    private ListBytesInput buf;
    private final Compressor compressor;
    private final MeasurementDescriptor desc;

    private long totalValueCount;
    private long maxTimestamp;
    private long minTimestamp = -1;

    public PageWriterImpl(MeasurementDescriptor desc) {
        this.desc = desc;
        this.compressor = desc.getCompressor();
        this.buf = new ListBytesInput();
    }

    @Override
    public void writePage(BytesInput bytesInput, int valueCount, Statistics<?> statistics,
                          long maxTimestamp, long minTimestamp) throws PageException {
        // compress the input data
        if (this.minTimestamp == -1)
            this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        long uncompressedSize = bytesInput.size();
        if (uncompressedSize > Integer.MAX_VALUE) {
            resetTimeStamp();
            throw new PageException("write too much bytes: " + uncompressedSize);
        }
        BytesInput compressedBytes = compressor.compress(bytesInput);
        long compressedSize = compressedBytes.size();
        if (compressedSize > Integer.MAX_VALUE) {
            resetTimeStamp();
            throw new PageException("write too much bytes: " + compressedSize);
        }
        BytesInput.PublicBAOS tempOutputStream = new BytesInput.PublicBAOS();
        // write the page header to IOWriter
        try {
            ReadWriteThriftFormatUtils.writeDataPageHeader((int) uncompressedSize,
                    (int) compressedSize, valueCount, statistics, valueCount,
                    desc.getEncodingType(), tempOutputStream, maxTimestamp, minTimestamp);
        } catch (IOException e) {
            resetTimeStamp();
            throw new PageException(
                    "meet IO Exception in writeDataPageHeader,ignore this page,error message:"
                            + e.getMessage());
        }
        this.totalValueCount += valueCount;
        buf.appendPublicBAOS(tempOutputStream);
        if (compressedBytes instanceof ListBytesInput)
            buf.appendListBytesInput((ListBytesInput) compressedBytes);
        else
            try {
                buf.appendBytesInput(compressedBytes);
            } catch (IOException e) {
                // remove the written page header in buffer and totalValueCount
                this.totalValueCount -= valueCount;
                resetTimeStamp();
                buf.removeLast();
                throw new PageException(
                        "meet IO Exception in buffer appendBytesInput,ignore this page,error message:"
                                + e.getMessage());
            }
        LOG.debug("page {}:write page from seriesWriter, valueCount:{}, stats:{},size:{}", desc,
                valueCount, statistics, estimateMaxPageMemSize());
    }

    private void resetTimeStamp() {
        if(totalValueCount == 0)
            minTimestamp = -1;
    }

    @Override
    public void writeToFileWriter(TSFileIOWriter writer, Statistics<?> statistics)
            throws IOException {
        writer.startSeries(desc, compressor.getCodecName(), desc.getType(), statistics,
                maxTimestamp, minTimestamp);
        long totalByteSize = writer.getPos();
        writer.writeBytesToStream(buf);
        LOG.debug("write series to file finished:{}", desc);
        long size = writer.getPos() - totalByteSize;
        writer.endSeries(size, totalValueCount);
        LOG.debug(
                "page {}:write page to fileWriter,type:{},maxTime:{},minTime:{},nowPos:{},stats:{}",
                desc.getMeasurementId(), desc.getType(), maxTimestamp, minTimestamp,
                writer.getPos(), statistics);
    }

    @Override
    public void reset() {
        minTimestamp = -1;
        buf.clear();
        totalValueCount = 0;
    }

    @Override
    public long estimateMaxPageMemSize() {
        // return size of buffer + page max size;
        int digestSize = (totalValueCount==0) ? 0 : desc.getTypeLength() * 2;
        return buf.size()
                + TSFileIOWriter.metadataConverter.caculatePageHeaderSize(digestSize);
    }
}
