package cn.edu.tsinghua.tsfile.timeseries.filter.utils;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSAnd;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.CSOr;
import cn.edu.tsinghua.tsfile.timeseries.read.RecordReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterUtils {

    /** default splitter of path to split delta object ID and measurement ID **/
    private static final char PATH_SPLITER = '.';

    /**
     * split input exp into deltaObject, measurement, filter type and exp
     * exp-format: deltaObject.measurement,type,exp
     *
     * @param exp
     * @param recordReader
     * @return
     * @throws IOException
     */
    public static SingleSeriesFilterExpression construct(String exp, RecordReader recordReader) throws IOException{
        if (exp == null || exp.equals("null")) {
            return null;
        }
        String args[] = exp.split(",");
        // if deltaObjectId and measurementId doesn't exist, set them as null
        if (args[0].equals("0") || args[0].equals("1")) {
            return construct("null", "null", args[0], args[1], recordReader);
        }
        String s = args[1];
        String deltaObject = s.substring(0, s.lastIndexOf(PATH_SPLITER));
        String measurement = s.substring(s.lastIndexOf(PATH_SPLITER) + 1);
        return construct(deltaObject, measurement, args[0], args[2], recordReader);

    }

    /**
     * construct {@code SingleSeriesFilterExpression} by input deltaObject, measurement, filter type, exp and recordReader
     * @param deltaObject
     * @param measurement
     * @param filterType
     * @param exp
     * @param recordReader
     * @return
     * @throws IOException
     */
    public static SingleSeriesFilterExpression construct(String deltaObject, String measurement, String filterType,
                                                         String exp, RecordReader recordReader) throws IOException{

        // check if exp is null
        if (exp.equals("null")) {
            return null;
        }

        // analyze exp
        if (exp.charAt(0) != '(') {
            // judge if exp is equal expression
            boolean ifEq = exp.charAt(1) == '=' ? true : false;
            int type = Integer.valueOf(filterType);
            // offset is used to skip chars before '='
            int offset = ifEq ? 2 : 1;
            if (exp.charAt(0) == '=') { // construct equal expression
                if (type == 0) {    // long value
                    long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.eq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.TIME_FILTER), v);
                } else if (type == 1) { // float value
                    float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.eq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.FREQUENCY_FILTER), v);
                } else {
                    if (recordReader == null) {
                        // int value
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    }
                    // get corresponding {@code FilterSeries} of deltaObject and measurementId
                    FilterSeries<?> col = recordReader.getColumnByMeasurementName(deltaObject, measurement);
                    // init value by type of {@code FilterSeries}
                    if (col instanceof IntFilterSeries) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof BooleanFilterSeries) {
                        boolean v = Boolean.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.booleanFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof LongFilterSeries) {
                        long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof FloatFilterSeries) {
                        float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof DoubleFilterSeries) {
                        double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v);
                    } else if (col instanceof StringFilterSeries) {
                        String v = String.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.eq(FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), new Binary(v));
                    } else {
                        throw new UnSupportedDataTypeException("Construct FilterSeries: " + col);
                    }

                }
            } else if (exp.charAt(0) == '>') {  // construct gtEq expression
                if (type == 0) {    // long value
                    long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.TIME_FILTER), v, ifEq);
                } else if (type == 1) { // float value
                    float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.FREQUENCY_FILTER), v, ifEq);
                } else {
                    if (recordReader == null) {
                        // int value
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    }
                    // get corresponding {@code FilterSeries} of deltaObject and measurementId
                    FilterSeries<?> col = recordReader.getColumnByMeasurementName(deltaObject, measurement);
                    // init value by type of {@code FilterSeries}
                    if (col instanceof IntFilterSeries) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof LongFilterSeries) {
                        long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof FloatFilterSeries) {
                        float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof DoubleFilterSeries) {
                        double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof StringFilterSeries) {
                        String v = String.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.gtEq(FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), new Binary(v), ifEq);
                    } else {
                        throw new UnSupportedDataTypeException("Construct FilterSeries: " + col);
                    }

                }
            } else if (exp.charAt(0) == '<') {  // construct ltEq expression
                if (type == 0) {    // long value
                    long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.TIME_FILTER), v, ifEq);
                } else if (type == 1) { // float value
                    float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                    return FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.FREQUENCY_FILTER), v, ifEq);
                } else {
                    //default filter
                    if (recordReader == null) {
                        // int value
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    }
                    // get corresponding {@code FilterSeries} of deltaObject and measurementId
                    FilterSeries<?> col = recordReader.getColumnByMeasurementName(deltaObject, measurement);
                    // init value by type of {@code FilterSeries}
                    if (col instanceof IntFilterSeries) {
                        int v = Integer.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof LongFilterSeries) {
                        long v = Long.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof FloatFilterSeries) {
                        float v = Float.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof DoubleFilterSeries) {
                        double v = Double.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), v, ifEq);
                    } else if (col instanceof StringFilterSeries) {
                        String v = String.valueOf(exp.substring(offset, exp.length()).trim());
                        return FilterFactory.ltEq(FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER), new Binary(v), ifEq);
                    } else {
                        throw new UnSupportedDataTypeException("Construct FilterSeries: " + col);
                    }
                }
                // long v = Long.valueOf(exp.substring(offset,exp.length()).trim());
                // return FilterFactory.ltEq(FilterFactory.longColumn(deltaObject, measurement, ifTime), v, ifEq);
            }
            return null;
        }


        List<Character> operators = new ArrayList<Character>();
        List<SingleSeriesFilterExpression> filters = new ArrayList<>();

        /** current index of char in whole exp **/
        int idx = 0;
        /** number of '()' **/
        int numbracket = 0;
        /** indicate if expression is ltEq or gtEq **/
        boolean ltgtFlag = false;
        /** indicate if expression is Or or And **/
        boolean operFlag = false;

        /** current independent expression **/
        String texp = "";

        // split exp into multi filter expressions
        for (; idx < exp.length(); idx++) {
            char c = exp.charAt(idx);
            if (Character.isWhitespace(c) || c == '\0') {
                continue;
            }
            if (c == '(') {
                numbracket++;
            }
            if (c == ')') {
                numbracket--;
            }
            if (c == '>' || c == '<') {
                ltgtFlag = true;
            }
            if (numbracket == 0 && (c == '|' || c == '&')) {
                operFlag = true;
            }

            // should be a independent expression
            if (ltgtFlag && numbracket == 0 && operFlag) {
                SingleSeriesFilterExpression filter = construct(deltaObject, measurement, filterType,
                        texp.substring(1, texp.length() - 1), recordReader);
                filters.add(filter);
                operators.add(c);
                numbracket = 0;
                ltgtFlag = false;
                operFlag = false;
                texp = "";
            } else {
                texp += c;
            }
        }
        // if expression still exists, construct it
        if (!texp.equals("")) {
            filters.add(construct(deltaObject, measurement, filterType, texp.substring(1, texp.length() - 1), recordReader));
        }

        // check if splitting is correct
        if (filters.size() - operators.size() != 1) {
            return null;
        }

        // combine all filters into one filter
        SingleSeriesFilterExpression filter = filters.get(0);
        for (int i = 0; i < operators.size(); i++) {
            // combine filters by Or and And
            if (operators.get(i) == '|') {
                filter = (SingleSeriesFilterExpression) FilterFactory.or(filter, filters.get(i + 1));
            } else if (operators.get(i) == '&') {
                filter = (SingleSeriesFilterExpression) FilterFactory.and(filter, filters.get(i + 1));
            }
        }

        return filter;
    }

    /**
     * construct cross filter
     * @param exp
     * @param recordReader
     * @return
     * @throws IOException
     */
    public static FilterExpression constructCrossFilter(String exp, RecordReader recordReader) throws IOException {
        exp = exp.trim();

        if (exp.equals("null")) {
            return null;
        }

        // check if exp is not cross filter expression
        if (exp.charAt(0) != '[') {
            return construct(exp, recordReader);
        }

        /** number of '[]' **/
        int numbraket = 0;
        /** indicate if expression contains operator **/
        boolean operator = false;
        ArrayList<FilterExpression> filters = new ArrayList<>();
        ArrayList<Character> operators = new ArrayList<>();
        /** current independent expression of {@code SingleSeriesFilterExpression} **/
        String texp = "";

        // split exp into multi filter expressions
        for (int i = 0; i < exp.length(); i++) {
            char c = exp.charAt(i);

            if (Character.isWhitespace(c) || c == '\0') {
                continue;
            }

            if (c == '[') {
                numbraket++;
            }
            if (c == ']') {
                numbraket--;
            }
            if (numbraket == 0 && (c == '|' || c == '&')) {
                operator = true;
            }

            // should be a independent expression of {@code SingleSeriesFilterExpression}
            if (numbraket == 0 && operator) {
//    			System.out.println(texp);
//    			System.out.println(texp.length());
                FilterExpression filter = constructCrossFilter(texp.substring(1, texp.length() - 1), recordReader);
                filters.add(filter);
                operators.add(c);

                numbraket = 0;
                operator = false;
                texp = "";
            } else {
                texp += c;
            }
        }
        // if expression still exists, construct it
        if (!texp.equals("")) {
            filters.add(constructCrossFilter(texp.substring(1, texp.length() - 1), recordReader));
        }

        // if no operator exists in whole exp, warning
        if (operators.size() == 0) {
            //Warning TODO
            return new CSAnd(filters.get(0), filters.get(0));
        }

        // combine the first two {@code FilterExpression} together
        CrossSeriesFilterExpression csf;
        if (operators.get(0) == '|') {
            csf = new CSOr(filters.get(0), filters.get(1));
        } else {
            csf = new CSAnd(filters.get(0), filters.get(1));
        }

        // combine the rest {@code FilterExpression} together
        for (int i = 2; i < filters.size(); i++) {
            if (operators.get(i - 1) == '|') {
                csf = new CSOr(csf, filters.get(i));
            } else {
                csf = new CSAnd(csf, filters.get(i));
            }
        }
        return csf;
    }
}












