package com.alibaba.alink.operator.common.utils;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.alibaba.alink.common.linalg.DenseVector;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class PrettyDisplayUtils {

    static int LIST_NUM_EDGE_ITEMS = 3;
    static int MAP_NUM_EDGE_ITEMS = Integer.MAX_VALUE << 1;
    static String DEFAULT_DECIMAL_FORMAT = "#.####";
    static int DEFAULT_LINE_WIDTH = 75;

    /**
     * display any values with default settings
     * @param v
     * @param <T>
     * @return
     */
    public static <T> String display(T v) {
        return display(v, false);
    }

    public static <T> String display(T v, boolean useRawDoubleFormat) {
        Class<?> clz = v.getClass();
        if (List.class.isAssignableFrom(clz)) {
            return displayList((List) v);
        } else if (Map.class.isAssignableFrom(clz)) {
            return displayMap((Map)v);
        } else if (Double.class.isAssignableFrom(clz)) {
            return displayDouble((Double)v, useRawDoubleFormat);
        } else if (DenseVector.class.isAssignableFrom(clz)) {
            return displayDenseVector((DenseVector)v);
        }
        return v.toString();
    }

    /**
     * display a double value with given format
     * @param v
     * @param format+
     * @return
     */
    public static String displayDouble(double v, String format) {
        return displayDouble(v, format, false);
    }

    public static String displayDouble(double v, String format, boolean useRawDoubleFormat) {
        if (useRawDoubleFormat) {
            return String.valueOf(v);
        }

        DecimalFormat decimalFormat = new DecimalFormat(format);
        DecimalFormatSymbols decimalFormatSymbols
            = decimalFormat.getDecimalFormatSymbols();
        decimalFormatSymbols.setNaN("NaN");
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);
        return decimalFormat.format(v);
    }

    public static String displayDouble(double v) {
        return displayDouble(v, DEFAULT_DECIMAL_FORMAT);
    }

    public static String displayDouble(double v, boolean useRawDoubleFormat) {
        return displayDouble(v, DEFAULT_DECIMAL_FORMAT, useRawDoubleFormat);
    }

    /**
     * display a list
     * @param list list
     * @param numEdgeItems when the list is long, only first and last several items are displayed
     * @param linebreak whether to break line after each item
     * @param <T>
     * @return
     */
    public static <T> String displayList(List<T> list, int numEdgeItems, boolean linebreak) {
        StringBuilder sbd = new StringBuilder();
        String delimiter = linebreak ? ",\n" : ", ";
        if (list.size() <= 2 * numEdgeItems) {
            for (int i = 0; i < list.size(); i += 1) {
                sbd.append(display(list.get(i)));
                if (i < list.size() - 1) {
                    sbd.append(delimiter);
                }
            }
        } else {
            for (int i = 0; i < numEdgeItems; i += 1) {
                sbd.append(display(list.get(i)));
                sbd.append(delimiter);
            }
            sbd.append("...");
            sbd.append(delimiter);
            for (int i = list.size() - numEdgeItems; i < list.size(); i += 1) {
                sbd.append(display(list.get(i)));
                if (i < list.size() - 1) {
                    sbd.append(delimiter);
                }
            }
        }
        return prependStringWithIndent(sbd.toString(), "[") + "]";
    }

    public static <T> String displayList(List<T> list, boolean linebreak) {
        return displayList(list, LIST_NUM_EDGE_ITEMS, linebreak);
    }

    public static <T> String displayList(List<T> list) {
        return displayList(list, false);
    }

    /**
     * display a dense vector
     * @param dv
     * @return
     */
    public static String displayDenseVector(DenseVector dv) {
        double[] data = dv.getData();
        return display(Arrays.asList(ArrayUtils.toObject(data)));
    }

    /**
     * display a map
     * @param m
     * @param linebreak
     * @return
     */
    public static String displayMap(Map<?, ?> m, int numEdgeItems, boolean linebreak) {
        StringBuilder sbd = new StringBuilder();
        String delimiter = linebreak ? ",\n" : ", ";
        ArrayList<Map.Entry<?, ?>> list = new ArrayList<>(m.entrySet());
        if (list.size() <= 2 * numEdgeItems) {
            for (int i = 0; i < list.size(); i += 1) {
                Entry<?, ?> entry = list.get(i);
                sbd.append(display(entry.getKey()));
                sbd.append(": ");
                sbd.append(display(entry.getValue()));
                if (i < m.size() - 1) {
                    sbd.append(delimiter);
                }
            }
        } else {
            for (int i = 0; i < numEdgeItems; i += 1) {
                Entry<?, ?> entry = list.get(i);
                sbd.append(display(entry.getKey()));
                sbd.append(": ");
                sbd.append(display(entry.getValue()));
                sbd.append(delimiter);
            }
            sbd.append("...");
            sbd.append(delimiter);
            for (int i = list.size() - numEdgeItems; i < list.size(); i += 1) {
                Entry<?, ?> entry = list.get(i);
                sbd.append(display(entry.getKey()));
                sbd.append(": ");
                sbd.append(display(entry.getValue()));
                if (i < list.size() - 1) {
                    sbd.append(delimiter);
                }
            }
        }
        return prependStringWithIndent(sbd.toString(), "{") + "}";
    }

    public static String displayMap(Map<?, ?> m) {
        return displayMap(m, MAP_NUM_EDGE_ITEMS, true);
    }

    /**
     * Display 2D array as a table.
     * If the table is very large, you can truncate rows/columns in the middle before passing it.
     * @param table table data
     * @param nRows number of rows
     * @param nCols number of columns
     * @param rowNames names of rows, nullable
     * @param colNames columns of columns, nullable
     * @param cornerName name of the top-left corner, nullable
     * @param nRowEdgeItems number of first and last rows to be displayed
     * @param nColEdgeItems number of first and last columns to be displayed
     * @param <T>
     * @return
     */
    public static <T> String displayTable(T[][] table, int nRows, int nCols,
                                          String[] rowNames, String[] colNames, String cornerName,
                                          int nRowEdgeItems, int nColEdgeItems) {
        return displayTable(
            table, nRows, nCols, rowNames,
            colNames, cornerName, nRowEdgeItems,
            nColEdgeItems, false
        );
    }

    public static <T> String displayTable(T[][] table, int nRows, int nCols,
                                          String[] rowNames, String[] colNames, String cornerName,
                                          int nRowEdgeItems, int nColEdgeItems,
                                          boolean useRawDoubleFormat) {
        if (null == colNames) {
            cornerName = "";
        }
        int[] colWidth = new int[nCols];
        for (int i = 0; i < nCols; i += 1) {
            colWidth[i] = 3;
            if (null != colNames) {
                colWidth[i]= Math.max(colWidth[i], colNames[i].length());
            }
        }

        for (int i = 0; i < nRows; i += 1) {
            if ((i >= nRowEdgeItems) && (i < nRows - nRowEdgeItems)) {
                continue;
            }
            for (int j = 0; j < nCols; j += 1) {
                if ((j >= nColEdgeItems) && (j < nCols - nColEdgeItems)) {
                    continue;
                }
                colWidth[j] = Math.max(colWidth[j], display(table[i][j], useRawDoubleFormat).length());
            }
        }

        int rowNameWidth = 3;
        if (null != rowNames) {
            for (int i = 0; i < nRows; i += 1) {
                rowNameWidth = Math.max(rowNameWidth, rowNames[i].length());
            }
            if (null != cornerName) {
                rowNameWidth = Math.max(rowNameWidth, cornerName.length());
            }
        }

        StringBuilder sbd = new StringBuilder();

        if (null != colNames) {
            if (null != rowNames) {
                if (null == cornerName) {
                    cornerName = "";
                }
                sbd.append("|");
                sbd.append(StringUtils.leftPad(cornerName, rowNameWidth));
            }
            sbd.append("|");
            for (int j = 0; j < nCols; j += 1) {
                if ((j >= nColEdgeItems) && (j < nCols - nColEdgeItems)) {
                    if (j == nColEdgeItems) {
                        sbd.append("...");
                        sbd.append("|");
                    }
                    continue;
                }
                sbd.append(StringUtils.leftPad(colNames[j], colWidth[j]));
                sbd.append("|");
            }
            sbd.append("\n");

            if (null != rowNames) {
                sbd.append("|");
                sbd.append(StringUtils.leftPad("", rowNameWidth, "-"));
            }
            sbd.append("|");
            for (int j = 0; j < nCols; j += 1) {
                if ((j >= nColEdgeItems) && (j < nCols - nColEdgeItems)) {
                    if (j == nColEdgeItems) {
                        sbd.append("---");
                        sbd.append("|");
                    }
                    continue;
                }
                sbd.append(StringUtils.leftPad("", colWidth[j], "-"));
                sbd.append("|");
            }
            sbd.append("\n");
        }

        for (int i = 0; i < nRows; i += 1) {
            if ((i >= nRowEdgeItems) && (i < nRows - nRowEdgeItems)) {
                if (i == nRowEdgeItems) {
                    if (null != rowNames) {
                        sbd.append("|");
                        sbd.append(StringUtils.leftPad("...", rowNameWidth));
                    }
                    sbd.append("|");
                    for (int j = 0; j < nCols; j += 1) {
                        if ((j >= nColEdgeItems) && (j < nCols - nColEdgeItems)) {
                            if (j == nColEdgeItems) {
                                sbd.append("...");
                                sbd.append("|");
                            }
                        } else {
                            sbd.append(StringUtils.leftPad("...", colWidth[j]));
                            sbd.append("|");
                        }
                    }
                    sbd.append("\n");
                }
                continue;
            }

            if (null != rowNames) {
                sbd.append("|");
                sbd.append(StringUtils.leftPad(rowNames[i], rowNameWidth));
            }
            sbd.append("|");
            for (int j = 0; j < nCols; j += 1) {
                if ((j >= nColEdgeItems) && (j < nCols - nColEdgeItems)) {
                    if (j == nColEdgeItems) {
                        sbd.append("...");
                        sbd.append("|");
                    }
                    continue;
                }
                sbd.append(StringUtils.leftPad(display(table[i][j], useRawDoubleFormat), colWidth[j]));
                sbd.append("|");
            }
            sbd.append("\n");
        }
        return sbd.toString();
    }

    /**
     * Display 2D array as a table
     * @param table table data
     * @param nRows number of rows
     * @param nCols number of columns
     * @param rowNames names of rows, nullable
     * @param colNames columns of columns, nullable
     * @param cornerName name of the top-left corner, nullable
     * @param <T>
     * @return
     */
    public static <T> String displayTable(T[][] table, int nRows, int nCols,
                                          String[] rowNames, String[] colNames, String cornerName) {
        return displayTable(table, nRows, nCols, rowNames, colNames, cornerName, LIST_NUM_EDGE_ITEMS, LIST_NUM_EDGE_ITEMS);
    }

    /**
     * Split text into multiple lines, prepend spaces, and then join to one string.
     * @param text text using '\n' for linebreaks
     * @param indentSize the number of spaces to be prepended
     * @return
     */
    public static String indentLines(String text, int indentSize) {
        String indent = StringUtils.repeat(" ", indentSize);
        String[] lines = text.split("\n");
        return Arrays.stream(lines)
            .map(d -> indent + d)
            .collect(Collectors.joining("\n"));
    }

    /**
     * Split text into multiple lines, prepend s to the first line and spaces to other lines, and then join to one string.
     * The number of spaces is equal to the length of s
     * @param text text
     * @param s the string to be prepended in the first line
     * @return
     */
    public static String prependStringWithIndent(String text, String s) {
        int lens = s.length();
        String indentedText = indentLines(text, lens);
        return s + indentedText.substring(lens);
    }

    public static String displayHeadline(String head, Character padChar) {
        String ret = " " + head + " ";
        int paddingLength = (DEFAULT_LINE_WIDTH - ret.length()) / 2;
        ret = StringUtils.leftPad(ret, ret.length() + paddingLength, padChar);
        ret = StringUtils.rightPad(ret, ret.length() + paddingLength, padChar);
        ret += "\n";
        return ret;
    }
}
