package cn.edu.tsinghua.tsfile.tool;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.read.RecordReader;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;

import java.io.IOException;
import java.util.List;

/**
 * Upgrade a TsFile from version 30, corresponding IoTDB 0.3.1, to version 40, corresponding IoTDB 0.4.0.
 * Comparing to version 30, version 40 increases a list of aggregation statistics in
 * {@link cn.edu.tsinghua.tsfile.format.PageHeader PageHeader}.
 */
public class TsFileUpgrader {
    private final ITsRandomAccessFileWriter fileWriterOutput;
    private final ITsRandomAccessFileReader fileReaderInput;
    private List<RowGroupReader> rowGroupList;

    public TsFileUpgrader(ITsRandomAccessFileWriter fileWriterOutput, ITsRandomAccessFileReader fileReaderInput) {
        this.fileWriterOutput = fileWriterOutput;
        this.fileReaderInput = fileReaderInput;
    }

    /**
     * Note that in this function, the upgrader load the file metadata in a whole which may result in out of memory.
     */
    public void upgrade() throws IOException {
        RecordReader recordReader = new RecordReader(fileReaderInput);
        rowGroupList = recordReader.getAllRowGroupReaders();

    }
}
