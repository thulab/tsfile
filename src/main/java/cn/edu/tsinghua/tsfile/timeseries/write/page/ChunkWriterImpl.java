package cn.edu.tsinghua.tsfile.timeseries.write.page;

import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.common.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.compress.Compressor;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * a implementation of {@linkplain IChunkWriter IChunkWriter}
 *
 * @author kangrong
 * @see IChunkWriter IChunkWriter
 */
public class ChunkWriterImpl implements IChunkWriter {
    private static Logger LOG = LoggerFactory.getLogger(ChunkWriterImpl.class);
    private final Compressor compressor;
    private final MeasurementDescriptor desc;

    /**
     * all pages of this column
     */
    private ListByteArrayOutputStream buf;
    private long totalValueCount;
    private long maxTimestamp;
    private long minTimestamp = -1;

    public ChunkWriterImpl(MeasurementDescriptor desc) {
        this.desc = desc;
        this.compressor = desc.getCompressor();
        this.buf = new ListByteArrayOutputStream();
    }

    @Override
    public void addPage(ListByteArrayOutputStream listByteArray, int valueCount, Statistics<?> statistics,
                        long maxTimestamp, long minTimestamp) throws PageException {
        // 1. update time statistics
        if (this.minTimestamp == -1)
            this.minTimestamp = minTimestamp;
        if(this.minTimestamp==-1){
        	LOG.error("Write page error, {}, minTime:{}, maxTime:{}",desc,minTimestamp,maxTimestamp);
        }
        this.maxTimestamp = maxTimestamp;

        // 2. compress data and create temp PBAOS by estimated page size
        int uncompressedSize = listByteArray.size();
        ListByteArrayOutputStream compressedBytes = compressor.compress(listByteArray);
        int compressedSize = compressedBytes.size();
        PublicBAOS tempOutputStream = new PublicBAOS(estimateMaxPageHeaderSize() + compressedSize);

        // 3. write the page header to temp PBAOS
        try {
            ReadWriteThriftFormatUtils.writeDataPageHeader(uncompressedSize, compressedSize, valueCount, statistics,
                    valueCount, desc.getEncodingType(), tempOutputStream, maxTimestamp, minTimestamp);
        } catch (IOException e) {
            resetTimeStamp();
            throw new PageException(
                    "meet IO Exception in writeDataPageHeader,ignore this page,error message:" + e.getMessage());
        }

        // 4. update data point num
        this.totalValueCount += valueCount;

        // 5. write page content to temp PBAOS
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

        // 6. add current page to buf
        buf.append(tempOutputStream);
        LOG.debug("page {}:write page from seriesWriter, valueCount:{}, stats:{},size:{}", desc, valueCount, statistics,
                estimateMaxPageMemSize());
    }

    private void resetTimeStamp() {
        if (totalValueCount == 0)
            minTimestamp = -1;
    }

    @Override
    public void writeToFileWriter(TsFileIOWriter writer, Statistics<?> statistics) throws IOException {
    	if(minTimestamp==-1){
    		LOG.error("Write page error, {}, minTime:{}, maxTime:{}",desc,minTimestamp,maxTimestamp);
    	}
    	// 1. start to write this column chunk
        writer.startColumnChunk(desc, compressor.getCodecName(), desc.getType(), statistics, maxTimestamp, minTimestamp);
        long totalByteSize = writer.getPos();

        // 2. write content of this column
        writer.writeBytesToStream(buf);
        LOG.debug("write column chunk to file finished:{}", desc);

        // 3. end writing this column chunk, update the column chunk metadata in memory
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
        return TsFileIOWriter.metadataConverter.calculatePageHeaderSize(digestSize);
    }
}
