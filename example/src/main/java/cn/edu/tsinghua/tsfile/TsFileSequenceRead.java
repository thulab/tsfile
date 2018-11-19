package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.MetaMarker;
import cn.edu.tsinghua.tsfile.file.footer.RowGroupFooter;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.PageDataReader;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TsFileSequenceRead {

    public static void main(String[] args) throws IOException {
        TsFileSequenceReader reader = new TsFileSequenceReader("test.tsfile");
        System.out.println("position: " + reader.getChannel().position());
        System.out.println(reader.readHeadMagic());
        System.out.println(reader.readTailMagic());
        TsFileMetaData metaData = reader.readFileMetadata();
        // Sequential reading of one RowGroup now follows this order:
        // first SeriesChunks (headers and data) in one RowGroup, then the RowGroupFooter
        // Because we do not know how many chunks a RowGroup may have, we should read one byte (the marker) ahead and
        // judge accordingly.
        while (reader.hasNextRowGroup()) {
            byte marker = reader.readMarker();
            switch (marker) {
                case MetaMarker.ChunkHeader:
                    ChunkHeader header = reader.readChunkHeader();
                    System.out.println("position: " + reader.getChannel().position());
                    System.out.println("chunk: " + header.getMeasurementID());
                    Decoder defaultTimeDecoder = Decoder.getDecoderByType(TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder),
                            TSDataType.INT64);
                    Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
                    for (int j = 0; j < header.getNumOfPages(); j++) {
                        PageHeader pageHeader = reader.readPageHeader(header.getDataType());
                        System.out.println("position: " + reader.getChannel().position());
                        System.out.println("points in the page: " + pageHeader.getNumOfValues());
                        ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                        System.out.println("position: " + reader.getChannel().position());
                        System.out.println("page data size: " + pageHeader.getUncompressedSize() + "," + pageData.remaining());
                        PageDataReader reader1 = new PageDataReader(pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                        while (reader1.hasNext()) {
                            TimeValuePair pair = reader1.next();
                            System.out.println("time, value: " + pair.getTimestamp() + "," + pair.getValue());
                        }
                    }
                    break;
                case MetaMarker.RowGroupFooter:
                    RowGroupFooter rowGroupFooter = reader.readRowGroupFooter();
                    System.out.println("position: " + reader.getChannel().position());
                    System.out.println("row group: " + rowGroupFooter.getDeltaObjectID());
                    break;
                default:
                    MetaMarker.handleUnexpectedMarker(marker);
            }
        }
        reader.close();
    }
}
