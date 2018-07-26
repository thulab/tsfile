package cn.edu.tsinghua.tsfile.tool;

import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.header.RowGroupHeader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.PageDataReader;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author East
 */
public class TsFileAnalyzer {

    private static final int FOOTER_LENGTH = Integer.BYTES;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    private static final double SCALE = 0.05;
    private static final int BOX_NUM = 20;
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private String tsFilePath;
    private TsFileSequenceReader tsFileReader;

    private int fileSize;
    private int dataSize;
    private int metadataSize;
    private int filePathNum;
    private int fileRowNum;
    private long fileTimestampMin;
    private long fileTimestampMax;

    private int fileMetadataSize;
    private TsFileMetaData fileMetaData;
    private List<Integer> deltaObjectMetaDataSizeList;
    private List<Integer> deltaObjectMetaDataContentList;
    private List<Integer> rowGroupMetaDataSizeList;
    private List<Integer> rowGroupMetaDataContentList;
    private List<Integer> timeSeriesChunkMetaDataSizeList;
    private List<Integer> timeSeriesChunkMetaDataContentList;
    private List<Integer> pageSizeList;
    private List<Integer> pageContentList;

    private FileWriter outputWriter;
    private int tageNum = 0;

    public TsFileAnalyzer(String tsFilePath) throws IOException {
        this.tsFilePath = tsFilePath;

        dataSize = 0;
        metadataSize = 0;
        filePathNum = 0;
        fileRowNum = 0;
        fileTimestampMin = Long.MAX_VALUE;
        fileTimestampMax = Long.MIN_VALUE;
        deltaObjectMetaDataSizeList = new ArrayList<>();
        deltaObjectMetaDataContentList = new ArrayList<>();
        rowGroupMetaDataSizeList = new ArrayList<>();
        rowGroupMetaDataContentList = new ArrayList<>();
        timeSeriesChunkMetaDataSizeList = new ArrayList<>();
        timeSeriesChunkMetaDataContentList = new ArrayList<>();
        pageSizeList = new ArrayList<>();
        pageContentList = new ArrayList<>();
    }

    public void analyze() throws IOException {
        tsFileReader = new TsFileSequenceReader(tsFilePath);
        tsFileReader.open();

        fileSize = tsFileReader.getFileSize();
        fileMetadataSize = tsFileReader.getFileMetadtaSize();
        fileMetaData = tsFileReader.readFileMetadata();
        metadataSize += fileMetadataSize + MAGIC_LENGTH + MAGIC_LENGTH + Integer.BYTES;

        int totalCount = 0;
        for(Map.Entry<String, TsDeltaObjectMetadata> entry : fileMetaData.getDeltaObjectMap().entrySet()){
            TsDeltaObjectMetadata deltaObjectMetadata = entry.getValue();
            deltaObjectMetaDataSizeList.add(deltaObjectMetadata.getSerializedSize());
            deltaObjectMetaDataContentList.add(deltaObjectMetadata.getRowGroups().size());
            totalCount += deltaObjectMetadata.getRowGroups().size();
        }

        int rgCount = 0;
        while (tsFileReader.hasNextRowGroup()) {
            RowGroupHeader rowGroupHeader = tsFileReader.readRowGroupHeader();
            rowGroupMetaDataSizeList.add(rowGroupHeader.getSerializedSize());
            rowGroupMetaDataContentList.add(rowGroupHeader.getNumberOfChunks());
            metadataSize += rowGroupHeader.getSerializedSize();

            Set<String> measurementIdSet = new HashSet<>();
            for (int i = 0; i < rowGroupHeader.getNumberOfChunks(); i++) {
                ChunkHeader chunkHeader = tsFileReader.readChunkHeader();

                timeSeriesChunkMetaDataSizeList.add(chunkHeader.getSerializedSize());
                timeSeriesChunkMetaDataContentList.add(chunkHeader.getNumOfPages());
                measurementIdSet.add(chunkHeader.getMeasurementID());
                metadataSize += chunkHeader.getSerializedSize();

                for (int j = 0; j < chunkHeader.getNumOfPages(); j++) {
                    PageHeader pageHeader = tsFileReader.readPageHeader(chunkHeader.getDataType());
                    tsFileReader.readPage(pageHeader, chunkHeader.getCompressionType());
                    metadataSize += pageHeader.getSerializedSize();

                    fileRowNum += pageHeader.getNumOfValues();

                    if (fileTimestampMin > pageHeader.getMin_timestamp())
                        fileTimestampMin = pageHeader.getMin_timestamp();
                    if (fileTimestampMax < pageHeader.getMax_timestamp())
                        fileTimestampMax = pageHeader.getMax_timestamp();
                    System.out.println(111111);
                    System.out.println(pageHeader.getMax_timestamp());
                    System.out.println(pageHeader.getMin_timestamp());

                    pageSizeList.add(pageHeader.getCompressedSize());
                    pageContentList.add(pageHeader.getNumOfValues());
                    dataSize += pageHeader.getCompressedSize();
                }
            }

            filePathNum += measurementIdSet.size();
            rgCount++;
            System.out.println(rgCount / (float) totalCount);
        }

        tsFileReader.close();
    }

    private void writeTag() throws IOException {
        for (int i = 0; i < tageNum; i++)
            outputWriter.write("\t");
    }

    private void writeBeginTag(String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">\n");
        tageNum++;
    }

    private void writeEndTag(String key) throws IOException {
        tageNum--;
        writeTag();
        outputWriter.write("</" + key + ">\n");
    }

    private void writeContent(int content, String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">");
        outputWriter.write("" + content);
        outputWriter.write("</" + key + ">\n");
    }

    private void writeContent(double content, String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">");
        outputWriter.write("" + content);
        outputWriter.write("</" + key + ">\n");
    }

    private void writeContent(String content, String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">");
        outputWriter.write(content);
        outputWriter.write("</" + key + ">\n");
    }

    private void writeOneBox(int start, int end, int num, float rate) throws IOException {
        writeContent(start + "~" + end + "(" + (end - start + 1) + "):" + num + "," + rate * 100 + "%", "box");
    }

    private List<Long> getSimpleStatistics(List<Integer> dataList) {
        long min, max, sum;
        min = dataList.get(0);
        max = dataList.get(0);
        sum = 0;

        for (int data : dataList) {
            if (min > data) min = data;
            if (max < data) max = data;
            sum += data;
        }

        List<Long> res = new ArrayList<>();
        res.add(min);
        res.add(max);
        res.add(sum);
        return res;
    }

    private void writeStatistics(List<Integer> dataList, String name, String cate) throws IOException {
        List<Long> statistics = getSimpleStatistics(dataList);
        writeBeginTag(name + "_metadata_" + cate);
        writeContent(statistics.get(2) / (float) dataList.size(), "average");
        writeContent(statistics.get(0), "min");
        writeContent(statistics.get(1), "max");
        writeEndTag(name + "_metadata_" + cate);
    }

    private List<Integer> getCountList(List<Integer> dataList, List<Integer> boxList) {
        List<Integer> counts = new ArrayList<>();

        int index = 0;
        int count = 0;
        for (int box : boxList) {
            while (index < dataList.size() && box > dataList.get(index)) {
                index++;
                count++;
            }

            counts.add(count);
            count = 0;
        }

        return counts;
    }

    private List<Integer> getBoxList(List<Integer> dataList) {
        List<Integer> boxList = new ArrayList<>();

        int scalesize = (int) (dataList.size() * SCALE);
        int max = dataList.get(dataList.size() - 1);
        int start = dataList.get(0);
        int end = dataList.get(dataList.size() - 1 - scalesize);
        int shift = (end - start) / BOX_NUM;
        if (shift <= 0) shift = 1;

        if (max - start + 1 <= BOX_NUM) {
            for (int i = start + 1; i <= max + 1; i++) {
                boxList.add(i);
            }
        } else if (end - start + 1 <= BOX_NUM) {
            for (int i = start + 1; i <= end; i++) {
                boxList.add(i);
            }
            if (max > end) boxList.add(max);

            int temp = boxList.remove(boxList.size() - 1);
            boxList.add(temp + 1);
        } else {
            for (int i = 1; i < BOX_NUM; i++) {
                boxList.add(start + shift * i);
            }
            boxList.add(end);
            if (end < max) boxList.add(max);

            int temp = boxList.remove(boxList.size() - 1);
            boxList.add(temp + 1);
        }

        return boxList;
    }

    private void writeDistribution(List<Integer> dataList, String name, String cate) throws IOException {
        writeBeginTag(name + "_metadata_" + cate);

        Collections.sort(dataList);
        List<Integer> boxList = getBoxList(dataList);
        List<Integer> countList = getCountList(dataList, boxList);

        int start = dataList.get(0);
        for (int i = 0; i < Math.min(countList.size(), boxList.size()); i++) {
            writeOneBox(start, boxList.get(i) - 1, countList.get(i), countList.get(i) / (float) dataList.size());
            start = boxList.get(i);
        }

        writeEndTag(name + "_metadata_" + cate);
    }

    public void output(String filename) throws IOException {
        outputWriter = new FileWriter(filename);

        // file
        writeBeginTag("File");
        writeContent(tsFilePath, "file_path");
        writeContent(fileSize, "file_size");
        writeContent(fileSize / Math.pow(1024, 3), "file_size_GB");
        writeContent(dataSize, "data_size");
        writeContent(metadataSize, "metadata_size");
        writeContent(dataSize / (double) fileSize, "data_rate");
        writeContent(deltaObjectMetaDataSizeList.size(), "file_deltaobject_num");
        writeContent(fileMetaData.getTimeSeriesList().size(), "file_timeseries_num");
        writeContent(filePathNum, "file_path_num");
        writeContent(fileRowNum, "file_points_num");
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        writeContent(dateFormat.format(new Date(fileTimestampMin)), "file_timestamp_min");
        writeContent(dateFormat.format(new Date(fileTimestampMax)), "file_timestamp_max");
        writeEndTag("File");

        // file metadata
        writeBeginTag("FileMetaData");
        writeContent(1, "file_metadata_num");
        writeContent(fileMetadataSize, "file_metadata_size");
        writeContent(deltaObjectMetaDataSizeList.size(), "file_metadata_children_num");
        writeEndTag("FileMetaData");

        // row groups of deltaobject metadata
        writeBeginTag("RowGroupsOfDeltaObjectMetaData");
        writeContent(deltaObjectMetaDataSizeList.size(), "rowgroupsofdeltaobject_metadata_num");
        writeStatistics(deltaObjectMetaDataSizeList, "rowgroupsofdeltaobject", "size");
        writeDistribution(deltaObjectMetaDataSizeList, "rowgroupsofdeltaobject", "size_distribution");
        writeStatistics(deltaObjectMetaDataSizeList, "rowgroupsofdeltaobject", "children_num");
        writeDistribution(deltaObjectMetaDataContentList, "rowgroupsofdeltaobject", "children_num_distribution");
        writeEndTag("RowGroupsOfDeltaObjectMetaData");

        // row group metadata
        writeBeginTag("RowGroup");
        writeContent(rowGroupMetaDataSizeList.size(), "rowgroup_metadata_num");
        writeStatistics(rowGroupMetaDataSizeList, "rowgroup", "size");
        writeDistribution(rowGroupMetaDataSizeList, "rowgroup", "size_distribution");
        writeStatistics(rowGroupMetaDataContentList, "rowgroup", "children_num");
        writeDistribution(rowGroupMetaDataContentList, "rowgroup", "children_num_distribution");
        writeEndTag("RowGroup");

        // time series chunk metadata
        writeBeginTag("TimeSeriesChunk");
        writeContent(timeSeriesChunkMetaDataSizeList.size(), "timeserieschunk_metadata_num");
        writeStatistics(timeSeriesChunkMetaDataSizeList, "timeserieschunk", "size");
        writeDistribution(timeSeriesChunkMetaDataSizeList, "timeserieschunk", "size_distribution");
        writeStatistics(timeSeriesChunkMetaDataContentList, "timeserieschunk", "children_num");
        writeDistribution(timeSeriesChunkMetaDataContentList, "timeserieschunk", "children_num_distribution");
        writeEndTag("TimeSeriesChunk");

        // page
        writeBeginTag("Page");
        writeContent(pageSizeList.size(), "page_metadata_num");
        writeStatistics(pageSizeList, "page", "size");
        writeDistribution(pageSizeList, "page", "size_distribution");
        writeStatistics(pageContentList, "page", "children_num");
        writeDistribution(pageContentList, "page", "children_num_distribution");
        writeEndTag("Page");

        outputWriter.close();
    }

    public static void main(String[] args) throws IOException {
//        if (args == null || args.length < 2) {
//            System.out.println("[ERROR] Too few params input, please input path for both tsfile and output report.");
//            return;
//        }
//
//        String inputFilePath = args[0];
//        String outputFilePath = args[1];
        String inputFilePath = "test.ts";
        String outputFilePath = "report.txt";
        TsFileAnalyzer analyzer = new TsFileAnalyzer(inputFilePath);
        analyzer.analyze();
        analyzer.output(outputFilePath);
    }
}
