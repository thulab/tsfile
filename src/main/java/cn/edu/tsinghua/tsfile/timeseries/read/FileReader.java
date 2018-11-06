package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * This class is used to read <code>TSFileMetaData</code> and construct
 * file level reader which contains the information of <code>RowGroupReader</code>.
 *
 * @author Jinrui Zhang
 */
public class FileReader {
    private static final Logger logger = LoggerFactory.getLogger(FileReader.class);

    /** length of a int value **/
    private static final int FOOTER_LENGTH = 4;
    /** the version info length **/
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    /** length threshold of the LRU cache **/
    private static final int LRU_LENGTH = 1000000;  // TODO: get this from a configuration
    /**
     * If the file has many rowgroups and series,
     * the storage of <code>fileMetaData</code> may be large.
     */
    private TsFileMetaData fileMetaData;
    /** actual tsfile reader **/
    private ITsRandomAccessFileReader randomAccessFileReader;

    /** readers of all RowGroupMetaDatas **/
    private Map<String, List<RowGroupReader>> rowGroupReaderMap;

    /** LRU cache of RowGroup readers **/
    // TODO: do we need to manage RowGroupReaders across files?
    private LinkedList<String> rowGroupReaderLRUList;

    /**
     * Lock when initializing RowGroupReaders so that the same deltaObj will not be initialized more than once.
     */
    private ReentrantReadWriteLock rwLock;

    /**
     * init from nothing
     * @param raf input reader of this tsfile
     * @throws IOException
     */
    public FileReader(ITsRandomAccessFileReader raf) throws IOException {
        this.randomAccessFileReader = raf;
        this.rwLock = new ReentrantReadWriteLock();
        this.rowGroupReaderLRUList = new LinkedList<>();
        init();
    }

    /**
     * init from list of RowGroupMetaDatas
     * Used for IoTDB compatibility
     *
     * @param reader input reader of this tsfile
     * @param rowGroupMetaDataList input list of RowGroupMetaDatas
     */
    public FileReader(ITsRandomAccessFileReader reader, List<RowGroupMetaData> rowGroupMetaDataList) throws IOException {
        this.randomAccessFileReader = reader;
        this.rwLock = new ReentrantReadWriteLock();
        this.rowGroupReaderLRUList = new LinkedList<>();
        initFromRowGroupMetadataList(rowGroupMetaDataList);
    }

    /**
     * <code>FileReader</code> initialization, construct <code>fileMetaData</code>
     * <code>rowGroupReaderList</code>, and <code>rowGroupReaderMap</code>.
     *
     * @throws IOException file read error
     */
    private void init() throws IOException {
        // seek to the bottom of tsfile to read FileMetaData
        long l = randomAccessFileReader.length();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        // read FileMetaData length
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        // read FileMetaData
        randomAccessFileReader.read(buf, 0, buf.length);//FIXME  is this a potential bug?

        // construct FileMetaData
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        this.fileMetaData = new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(bais));

        // init {@code rowGroupReaderMap}
        rowGroupReaderMap = new HashMap<>();
    }

    /**
     * //TODO verify rightness
     * Used for IoTDB compatibility
     * init by list of RowGroupMetaDatas
     *
     * @param rowGroupMetadataList
     */
    private void initFromRowGroupMetadataList(List<RowGroupMetaData> rowGroupMetadataList) {
        rowGroupReaderMap = new HashMap<>();
        // loop all RowGroupMetaDatas and update LRU cache by {@code deltaObjectID}
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
            String deltaObjectID = rowGroupMetaData.getDeltaObjectID();
            updateLRU(deltaObjectID);
        }
        // init readers of each RowGroupMetaData
        initRowGroupReaders(rowGroupMetadataList);
    }

    /**
     * the {@code rowGroupReaderMap} of this FileReader
     * Do not use this method for potential risks of LRU cache overflow.
     *
     * @return this.rowGroupReaderMap
     */
    @Deprecated
    public Map<String, List<RowGroupReader>> getRowGroupReaderMap() {
        // if {@code fileMetaData} is not constructed yet, return {@code rowGroupReaderMap} directly
        if (this.fileMetaData == null) {
            return rowGroupReaderMap;
        }

        // else update {@code rowGroupReaderMap} first, and then return it
        try {
            loadAllDeltaObj();
        } catch (IOException e) {
            logger.error("cannot get all RowGroupReaders because {}", e.getMessage());
        }
        return this.rowGroupReaderMap;
    }

    public Map<String, String> getProps() {
        return fileMetaData.getProps();
    }

    public String getProp(String key) {
        return fileMetaData.getProp(key);
    }

    /**
     * Get all readers that access every RowGroup belonging to deltaObjectUID within this file.
     * This method will try to init the readers if they are uninitialized(non-exist).
     *
     * @param deltaObjectUID name of the desired deltaObject
     * @return A list of RowGroupReaders specified by deltaObjectUID
     * or NULL if such deltaObject doesn't exist in this file
     */
    public List<RowGroupReader> getRowGroupReaderListByDeltaObject(String deltaObjectUID) throws IOException {
        loadDeltaObj(deltaObjectUID);
        return this.rowGroupReaderMap.get(deltaObjectUID);
    }

    public List<RowGroupReader> getRowGroupReaderListByDeltaObjectByHadoop(String deltaObjectUID) throws IOException {
        return this.rowGroupReaderMap.get(deltaObjectUID);
    }

    /**
     * get corresponding TSDataType from RowGroupMetaDatas
     * @param deltaObject
     * @param measurement
     * @return
     * @throws IOException
     */
    public TSDataType getDataTypeBySeriesName(String deltaObject, String measurement) throws IOException {
        loadDeltaObj(deltaObject);
        List<RowGroupReader> rgrList = getRowGroupReaderMap().get(deltaObject);
        if (rgrList == null || rgrList.size() == 0) {
            return null;
        }
        return rgrList.get(0).getDataTypeBySeriesName(measurement);
    }

    /**
     * close this FileReader
     * @throws IOException
     */
    public void close() throws IOException {
        this.randomAccessFileReader.close();
    }

    /* The below methods can be used to init RowGroupReaders of a given deltaObj
        in different ways, in case of another refactoring. Current method is based on TsDeltaObject.
    */

    /**
     * This method is thread-safe.
     *
     * @param deltaObjUID
     * @throws IOException
     */
    private void initRowGroupReaders(String deltaObjUID) throws IOException {
        // if already exists, just return it, avoid duplicates
        if (this.rowGroupReaderMap.containsKey(deltaObjUID))
            return;

        // init new RowGroup reader by FileMetaData
        this.rwLock.writeLock().lock();
        try {
            TsDeltaObject deltaObj = this.fileMetaData.getDeltaObject(deltaObjUID);
            initRowGroupReaders(deltaObj);
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    /**
     * This method is thread-unsafe, so the caller must ensure thread safety.
     *
     * @param deltaObj TSDeltaObject that contains a list of RowGroupMetaData
     * @throws IOException
     */
    private void initRowGroupReaders(TsDeltaObject deltaObj) throws IOException {
        if (deltaObj == null)
            return;
        // read metadata block and use its RowGroupMetadata list to construct RowGroupReaders
        TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
        blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(this.randomAccessFileReader,
                deltaObj.offset, deltaObj.metadataBlockSize));
        initRowGroupReaders(blockMeta.getRowGroups());
    }

    /**
     * Core method, construct RowGroupReader for every RowGroup in given list, thread-unsafe.
     * The caller should avoid adding duplicate readers.
     *
     * @param groupList
     */
    private void initRowGroupReaders(List<RowGroupMetaData> groupList) {
        if (groupList == null)
            return;
        // TODO: advice: parallel the process to speed up
        for (RowGroupMetaData meta : groupList) {
            // the passed raf should be new rafs to realize parallelism

            // init RowGroupReader
            RowGroupReader reader = new RowGroupReader(meta, this.randomAccessFileReader);

            // update {@code rowGroupReaderMap}
            List<RowGroupReader> readerList = this.rowGroupReaderMap.get(meta.getDeltaObjectID());
            if (readerList == null) {
                readerList = new ArrayList<>();
                rowGroupReaderMap.put(meta.getDeltaObjectID(), readerList);
            }
            readerList.add(reader);
        }
    }

    /*
        Belows are methods for maintaining LRU List. Is using an interface or base class better?
     */

    /**
     * Add a deltaObj by its name to the tail of the LRU list. If the deltaObj already exists,
     * remove it. When adding a new item, check if the volume exceeds, if so, remove the head of
     * list and responding RowGroupReaders.
     *
     * @param deltaObjUID
     */
    private void updateLRU(String deltaObjUID) {
        int idx = this.rowGroupReaderLRUList.indexOf(deltaObjUID);
        if (idx != -1) {
            // not a new item
            this.rowGroupReaderLRUList.remove(idx);
        } else {
            // a new item
            if (this.rowGroupReaderLRUList.size() > this.LRU_LENGTH) {
                String removedDeltaObj = this.rowGroupReaderLRUList.removeFirst();
                this.rowGroupReaderMap.remove(removedDeltaObj);
            }
        }
        this.rowGroupReaderLRUList.addLast(deltaObjUID);
    }

    @Deprecated
    /**
     * get all RowGroupReader in {@code rowGroupReaderMap}
     * only used for compatibility, such as spark
     */
    public List<RowGroupReader> getRowGroupReaderList() throws IOException {
        if (this.rowGroupReaderMap == null || this.rowGroupReaderMap.size() == 0) {
            loadAllDeltaObj();
        }

        List<RowGroupReader> ret = new ArrayList<>();
        for (Map.Entry<String, List<RowGroupReader>> entry : this.rowGroupReaderMap.entrySet()) {
            ret.addAll(entry.getValue());
        }
        return ret;
    }

    /**
     * This method prefetch metadata of a DeltaObject for methods like checkSeries,
     * if the DeltaObject is not in memory.
     *
     * @param deltaObjUID
     */
    public void loadDeltaObj(String deltaObjUID) throws IOException {
        // check if this file do have this delta_obj
        if (!this.fileMetaData.containsDeltaObject(deltaObjUID)) {
            return;
        }
        List<RowGroupReader> ret = rowGroupReaderMap.get(deltaObjUID);
        if (ret == null) {
            initRowGroupReaders(deltaObjUID);
        }
        updateLRU(deltaObjUID);
    }

    /**
     * init readers for all RowGroupMetaDatas in {@code fileMetaData}
     * @throws IOException
     */
    private void loadAllDeltaObj() throws IOException {
        Collection<String> deltaObjects = fileMetaData.getDeltaObjectMap().keySet();
        for (String deltaObject : deltaObjects) {
            initRowGroupReaders(deltaObject);
        }
    }

    /**
     * check if {@code fileMetaData} contains input deltaObjUID
     * @param deltaObjUID
     * @return
     */
    public boolean containsDeltaObj(String deltaObjUID) {
        return this.fileMetaData.containsDeltaObject(deltaObjUID);
    }

    /**
     * check if {@code rowGroupReaderMap} contains input deltaObjUID and measurementID
     * @param deltaObjUID
     * @param measurementID
     * @return
     * @throws IOException
     */
    public boolean containsSeries(String deltaObjUID, String measurementID) throws IOException {
        if (!this.containsDeltaObj(deltaObjUID)) {
            return false;
        } else {
            this.loadDeltaObj(deltaObjUID);
            List<RowGroupReader> readers = rowGroupReaderMap.get(deltaObjUID);
            for (RowGroupReader reader : readers) {
                if (reader.containsMeasurement(measurementID))
                    return true;
            }
        }
        return false;
    }

    public TsFileMetaData getFileMetaData() {
        return this.fileMetaData;
    }

    /**
     * get list of all RowGroupMetaData in {@code fileMetaData} order by file offset
     * only used by hadoop
     * @return
     * @throws IOException
     */
    public List<RowGroupMetaData> getSortedRowGroupMetaDataList() throws IOException{
        // get all RowGroupMetaData from {@code fileMetaData}
        List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
        Collection<String> deltaObjects = fileMetaData.getDeltaObjectMap().keySet();
        for (String deltaObjectID : deltaObjects) {
            this.rwLock.writeLock().lock();
            try {
                TsDeltaObject deltaObj = this.fileMetaData.getDeltaObject(deltaObjectID);
                TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
                blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(this.randomAccessFileReader,
                        deltaObj.offset, deltaObj.metadataBlockSize));
                rowGroupMetaDataList.addAll(blockMeta.getRowGroups());
            } finally {
                this.rwLock.writeLock().unlock();
            }
        }

        // order by file offset
        Comparator<RowGroupMetaData> comparator = new Comparator<RowGroupMetaData>() {
            @Override
            public int compare(RowGroupMetaData o1, RowGroupMetaData o2) {

                return Long.signum(o1.getMetaDatas().get(0).getProperties().getFileOffset() - o2.getMetaDatas().get(0).getProperties().getFileOffset());
            }

        };
        rowGroupMetaDataList.sort(comparator);
        return rowGroupMetaDataList;
    }

}
