package cn.edu.thu.tsfile.timeseries.read.qp;

import cn.edu.thu.tsfile.timeseries.utils.StringContainer;

/**
 * This class define an Object named Path to represent a series in delta system.
 * And in batch read, this definition is also used in query processing.
 * 
 * @author Kangrong
 *
 */
public class Path {
    private String measurement = null;
    private StringContainer deltaObject = null;
    private StringContainer fullPath;

    public Path(StringContainer pathSc) {
        String[] splits = pathSc.toString().split(SQLConstant.PATH_SEPARATER_NO_REGEX);
        fullPath = new StringContainer(splits, SQLConstant.PATH_SEPARATOR);
    }

    public Path(String pathSc) {
        String[] splits = pathSc.split(SQLConstant.PATH_SEPARATER_NO_REGEX);
        fullPath = new StringContainer(splits, SQLConstant.PATH_SEPARATOR);

    }

    public Path(String[] pathSc) {
        String[] splits =
                new StringContainer(pathSc, SQLConstant.PATH_SEPARATOR).toString().split(
                        SQLConstant.PATH_SEPARATER_NO_REGEX);
        fullPath = new StringContainer(splits, SQLConstant.PATH_SEPARATOR);

    }

    public String getFullPath() {
        return fullPath.toString();
    }

    public String getDeltaObjectToString() {
        if (deltaObject == null || measurement == null) {
            separateDeltaObjectMeasurement();
        }
        return deltaObject.join(SQLConstant.PATH_SEPARATOR);
    }

    public String getMeasurementToString() {
        if (deltaObject == null || measurement == null) {
            separateDeltaObjectMeasurement();
        }
        return measurement;
    }

    private void separateDeltaObjectMeasurement() {
        if (fullPath == null || fullPath.size() == 0) {
            deltaObject = new StringContainer();
            measurement = "";
        } else if (fullPath.size() == 1) {
            deltaObject = new StringContainer();
            measurement = fullPath.toString();
        } else {
            deltaObject = fullPath.getSubStringContainer(0, -2);
            measurement = fullPath.getSubString(-1);
        }
    }

    public static Path mergePath(Path prefix, Path suffix) {
        Path ret = new Path(prefix.fullPath.clone());
        ret.fullPath.addTail(suffix.fullPath);
        return ret;
    }
    @Override
    public int hashCode(){
    	return fullPath.toString().hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof Path))
            return false;
        return this.fullPath.toString().equals(((Path) obj).fullPath.toString());
    }

    public boolean equals(String obj) {
        if (obj == null)
            return false;
        return this.fullPath.toString().equals(obj);
    }

    @Override
    public String toString() {
        return fullPath.toString();
    }

    public void addHeadPath(String deltaObject) {
        String[] splits = deltaObject.split(SQLConstant.PATH_SEPARATER_NO_REGEX);
        fullPath.addHead(splits);
        deltaObject = null;
    }

    public void addHeadPath(Path prefix) {
        fullPath.addHead(prefix.fullPath);
        deltaObject = null;

    }

    @Override
    public Path clone() {
        StringContainer sc = this.fullPath.clone();
        return new Path(sc);
    }

    /**
     * if prefix is null, return false
     * 
     * @param prefix
     * @return
     */
    public boolean startWith(String prefix) {
        if (prefix == null)
            return false;
        return fullPath.toString().startsWith(prefix);
    }

    public boolean startWith(Path prefix) {
        if (prefix == null)
            return false;
        return fullPath.toString().startsWith(prefix.fullPath.toString());
    }

    public void replace(String srcPrefix, Path descPrefix) {
        if (!startWith(srcPrefix))
            return;
        int prefixSize = srcPrefix.split(SQLConstant.PATH_SEPARATER_NO_REGEX).length;
        StringContainer newPath = fullPath.getSubStringContainer(prefixSize, -1);
        newPath.addHead(descPrefix.fullPath);
        this.fullPath = newPath;
        deltaObject = null;
    }

}
