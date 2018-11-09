package cn.edu.tsinghua.tsfile.file.metadata;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

public class TsRowGroupBlockMetaData implements IConverter<RowGroupBlockMetaData> {
    /**
     * All row groups of this deltaObject in this file
     */
    private List<RowGroupMetaData> rowGroupMetadataList;

    /**
     * name of deltaObjectID
     */
    private String deltaObjectID;

    public TsRowGroupBlockMetaData() {
        rowGroupMetadataList = new ArrayList<>();
    }

    public TsRowGroupBlockMetaData(List<RowGroupMetaData> rowGroupMetadataList) {
        this.rowGroupMetadataList = rowGroupMetadataList;
    }

    /**
     * add row group metadata to rowGroups. THREAD NOT SAFE
     *
     * @param rowGroup - row group metadata to add
     */
    public void addRowGroupMetaData(RowGroupMetaData rowGroup) {
        if (rowGroupMetadataList == null) {
            rowGroupMetadataList = new ArrayList<>();
        }
        rowGroupMetadataList.add(rowGroup);
    }

    public List<RowGroupMetaData> getRowGroups() {
        return rowGroupMetadataList;
    }

    public void setRowGroups(List<RowGroupMetaData> rowGroupMetadataList) {
        this.rowGroupMetadataList = rowGroupMetadataList;
    }

    /**
     * Serialize class TsRowGroupBlockMetaData to thrift format
     *
     * @return class of thrift format
     */
    @Override
    public RowGroupBlockMetaData convertToThrift() {
        List<cn.edu.tsinghua.tsfile.format.RowGroupMetaData> rowGroupMetaDataListInThrift = null;
        if (rowGroupMetadataList != null) {
            rowGroupMetaDataListInThrift =
                    new ArrayList<>();
            //convert all rowGroupMetadata to thrift format
            for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
                rowGroupMetaDataListInThrift.add(rowGroupMetaData.convertToThrift());
            }
        }
        RowGroupBlockMetaData rowGroupBlockMetaData = new RowGroupBlockMetaData(rowGroupMetaDataListInThrift);
        rowGroupBlockMetaData.setDelta_object_id(deltaObjectID);
        return rowGroupBlockMetaData;
    }

    /**
     * Deserialize class TsRowGroupBlockMetaData from thrift format to normal format
     *
     * @param metadataInThrift thrift format of class TsRowGroupBlockMetaData
     */
    @Override
    public void convertToTSF(RowGroupBlockMetaData metadataInThrift) {
        List<cn.edu.tsinghua.tsfile.format.RowGroupMetaData> rowGroupMetaDataListInThrift =
                metadataInThrift.getRow_groups_metadata();
        if (rowGroupMetaDataListInThrift == null) {
            rowGroupMetadataList = null;
        } else {
            rowGroupMetadataList = new ArrayList<>();
            //convert all rowGroupMetadata from thrift format to normal format
            for (cn.edu.tsinghua.tsfile.format.RowGroupMetaData rowGroupMetaDataInThrift : rowGroupMetaDataListInThrift) {
                RowGroupMetaData rowGroupMetaDataInTSFile = new RowGroupMetaData();
                rowGroupMetaDataInTSFile.convertToTSF(rowGroupMetaDataInThrift);
                rowGroupMetadataList.add(rowGroupMetaDataInTSFile);
            }
        }
        this.deltaObjectID = metadataInThrift.getDelta_object_id();
    }

    public String getDeltaObjectID() {
        return deltaObjectID;
    }

    public void setDeltaObjectID(String deltaObjectID) {
        this.deltaObjectID = deltaObjectID;
    }
}
