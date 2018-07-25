package cn.edu.tsinghua.tsfile.timeseries.readV2;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.utils.ByteBufferUtil;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.header.RowGroupHeader;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.PageDataReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TsFileSequenceReader {
    private Path path;
    FileChannel channel;
    private long fileMetadataPos;
    private int fileMetadtaSize;

    public TsFileSequenceReader(String file){
        this.path= Paths.get(file);
    }
    /**
     * After open the file, the reader position is at the end of the  magic string in the header.
     * @return
     * @throws IOException
     */
    public void open() throws IOException {
        channel=FileChannel.open(path, StandardOpenOption.READ);
        ByteBuffer metadataSize=ByteBuffer.allocate(Integer.BYTES);
        channel.read(metadataSize, channel.size()-TSFileConfig.MAGIC_STRING.length()-Integer.BYTES);
        metadataSize.flip();
        fileMetadtaSize =ByteBufferUtil.toInt(metadataSize);//read file metadata size and position
        fileMetadataPos=channel.size()-TSFileConfig.MAGIC_STRING.length()-Integer.BYTES-fileMetadtaSize;
        channel.position(TSFileConfig.MAGIC_STRING.length());//skip the magic header
    }

    /**
     * this function does not modify the position of the file reader.
     * @return
     * @throws IOException
     */
    public String readTailMagic() throws IOException {
        long totalSize=channel.size();
        ByteBuffer magicStringBytes=ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        channel.read(magicStringBytes, totalSize-TSFileConfig.MAGIC_STRING.length());
        magicStringBytes.flip();
        return ByteBufferUtil.string(magicStringBytes);
    }
    /**
     * this function does not modify the position of the file reader.
     * @return
     * @throws IOException
     */
    public String readHeadMagic() throws IOException {
        ByteBuffer magicStringBytes=ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        channel.read(magicStringBytes, 0);
        magicStringBytes.flip();
        return ByteBufferUtil.string(magicStringBytes);
    }

    /**
     * this function does not modify the position of the file reader.
     * @return
     * @throws IOException
     */
    public TsFileMetaData readFileMetadata() throws IOException {
        ByteBuffer buffer=ByteBuffer.allocate(fileMetadtaSize);
        ReadWriteIOUtils.readAsPossible(channel,fileMetadataPos, buffer);
        buffer.flip();
        return TsFileMetaData.deserializeFrom(buffer);
    }

    public boolean hasNextRowGroup() throws IOException {
            return channel.position() < fileMetadataPos;
    }

    public RowGroupHeader readRowGroupHeader() throws IOException {
        return RowGroupHeader.deserializeFrom(Channels.newInputStream(channel));
    }
    public ChunkHeader readChunkHeader() throws IOException {
        return ChunkHeader.deserializeFrom(Channels.newInputStream(channel));
    }

    /**
     * notice, this function will modify channel's position.
     * @param offset the file offset of this chunk's header
     * @return
     * @throws IOException
     */
    public ChunkHeader readChunkHeader(long offset) throws IOException {
        channel.position(offset);
        return ChunkHeader.deserializeFrom(Channels.newInputStream(channel));
    }

    /**
     * notice, the position of the channel MUST be at the end of this header.
     * @param header
     * @return the pages of this chunk
     * @throws IOException
     */
    public ByteBuffer readChunk(ChunkHeader header) throws  IOException{
        ByteBuffer buffer=ByteBuffer.allocate(header.getDataSize());
        ReadWriteIOUtils.readAsPossible(channel, buffer);
        buffer.flip();
        return buffer;
    }

    /**
     * notice, this function will modify channel's position.
     * @param position
     * @return
     * @throws IOException
     */
    public ByteBuffer readChunkAndHeader(long position) throws  IOException{
        ChunkHeader header= readChunkHeader(position);
        ByteBuffer buffer = ByteBuffer.allocate(header.getSerializedSize() + header.getDataSize());
        header.serializeTo(buffer);
        ReadWriteIOUtils.readAsPossible(channel, buffer);
        buffer.flip();
        return buffer;
    }

    /**
     * notice, the function will midify channel's position.
     * @param position
     * @param length
     * @param output
     * @return
     * @throws IOException
     */
    public int readRaw(long position, int length, ByteBuffer output)  throws  IOException{
        channel.position(position);
        return ReadWriteIOUtils.readAsPossible(channel, output, length);
    }

    /**
     * notice, this function will modify channel's position.
     * @param header
     * @return the pages of this chunk
     * @throws IOException
     */
    public ByteBuffer readChunk(ChunkHeader header, long positionOfChunkHeader) throws  IOException{
        channel.position(positionOfChunkHeader);
        return readChunk(header);
    }

    public PageHeader readPageHeader(TSDataType type) throws IOException{
        return PageHeader.deserializeFrom(Channels.newInputStream(channel), type);
    }
    /**
     * notice, this function will modify channel's position.
     * @param offset the file offset of this page header's header
     * @return
     * @throws IOException
     */
    public PageHeader readPageHeader(TSDataType type, long offset) throws IOException {
        channel.position(offset);
        return PageHeader.deserializeFrom(Channels.newInputStream(channel), type);
    }

    public ByteBuffer readPage(PageHeader header, CompressionType type) throws  IOException{
        ByteBuffer buffer=ByteBuffer.allocate(header.getCompressedSize());
        ReadWriteIOUtils.readAsPossible(channel, buffer);
        buffer.flip();
        UnCompressor unCompressor=UnCompressor.getUnCompressor(type);
        ByteBuffer uncompressedBuffer= ByteBuffer.allocate(header.getUncompressedSize());
        unCompressor.uncompress(buffer, uncompressedBuffer);
        uncompressedBuffer.flip();
        return uncompressedBuffer;
    }


    public void close() throws IOException {
        this.channel.close();
    }

    public static void main(String[] args) throws IOException {
        TsFileSequenceReader reader=new TsFileSequenceReader("test.ts");
        reader.open();
        System.out.println("position: " + reader.channel.position());
        System.out.println(reader.readHeadMagic());
        System.out.println(reader.readTailMagic());
        TsFileMetaData metaData=reader.readFileMetadata();
        if(reader.hasNextRowGroup()){
            RowGroupHeader rowGroupHeader=reader.readRowGroupHeader();
            System.out.println("position: " + reader.channel.position());
            System.out.println("row group: " + rowGroupHeader.getDeltaObjectID());
            for(int i=0; i<rowGroupHeader.getNumberOfChunks();i++){
                ChunkHeader header=reader.readChunkHeader();
                System.out.println("position: " + reader.channel.position());
                System.out.println("chunk: "+ header.getMeasurementID());
                Decoder defaultTimeDecoder = Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT64);
                Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
                    for(int j=0; j<header.getNumOfPages();j++){
                        PageHeader pageHeader= reader.readPageHeader(header.getDataType());
                        System.out.println("position: " + reader.channel.position());
                        System.out.println("points in the page: "+pageHeader.getNumOfValues());
                        ByteBuffer pageData=reader.readPage(pageHeader, header.getCompressionType());
                        System.out.println("position: " + reader.channel.position());
                        System.out.println("page data size: "+ pageHeader.getUncompressedSize()+","+pageData.remaining());
                        PageDataReader reader1=new PageDataReader(pageData,header.getDataType(),valueDecoder,defaultTimeDecoder);
                        while(reader1.hasNext()){
                            TimeValuePair pair=reader1.next();
                            System.out.println("time, value: " + pair.getTimestamp()+","+pair.getValue());
                        }
                    }
            }
        }
        reader.close();
    }

}
