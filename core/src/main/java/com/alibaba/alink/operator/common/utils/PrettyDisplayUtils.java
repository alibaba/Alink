package com.alibaba.alink.operator.common.utils;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class PrettyDisplayUtils {

	static int LIST_NUM_EDGE_ITEMS = 3;
	static int MAP_NUM_EDGE_ITEMS = Integer.MAX_VALUE << 1;
	static String DEFAULT_DECIMAL_FORMAT = "#.####";
	static int DEFAULT_LINE_WIDTH = 75;
	static char DEFAULT_LIST_DELIMITER = ',';

	/**
	 * display any values with default settings
	 *
	 * @param v
	 * @param <T>
	 * @return
	 */
	public static <T> String display(T v) {
		return display(v, false);
	}

	public static <T> String display(T v, boolean useRawDoubleFormat) {
		if (null == v) {
			return "null";
		}
		Class <?> clz = v.getClass();
		if (List.class.isAssignableFrom(clz)) {
			return displayList((List <?>) v);
		} else if (Map.class.isAssignableFrom(clz)) {
			return displayMap((Map <?, ?>) v);
		} else if (Double.class.isAssignableFrom(clz)) {
			return displayDouble((Double) v, useRawDoubleFormat);
		} else if (DenseVector.class.isAssignableFrom(clz)) {
			return displayDenseVector((DenseVector) v);
		} else if (DenseMatrix.class.isAssignableFrom(clz)) {
			return displayDenseMatrix((DenseMatrix) v);
		}
		return v.toString();
	}

	/**
	 * display a double value with given format
	 *
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
	 *
	 * @param <T>
	 * @param list         list
	 * @param numEdgeItems when the list is long, only first and last several items are displayed
	 * @param linebreak    whether to break line after each item
	 * @param delimiter
	 * @return
	 */
	public static <T> String displayList(List <T> list, int numEdgeItems, boolean linebreak, char delimiter) {
		StringBuilder sbd = new StringBuilder();
		String delimiterStr = delimiter + (linebreak ? "\n" : " ");
		if (list.size() <= 2 * numEdgeItems) {
			for (int i = 0; i < list.size(); i += 1) {
				sbd.append(display(list.get(i)));
				if (i < list.size() - 1) {
					sbd.append(delimiterStr);
				}
			}
		} else {
			for (int i = 0; i < numEdgeItems; i += 1) {
				sbd.append(display(list.get(i)));
				sbd.append(delimiterStr);
			}
			sbd.append("...");
			sbd.append(delimiterStr);
			for (int i = list.size() - numEdgeItems; i < list.size(); i += 1) {
				sbd.append(display(list.get(i)));
				if (i < list.size() - 1) {
					sbd.append(delimiterStr);
				}
			}
		}
		return prependStringWithIndent(sbd.toString(), "[") + "]";
	}

	public static <T> String displayList(List <T> list, int numEdgeItems, boolean linebreak) {
		return displayList(list, numEdgeItems, linebreak, DEFAULT_LIST_DELIMITER);
	}

	public static <T> String displayList(List <T> list, boolean linebreak) {
		return displayList(list, LIST_NUM_EDGE_ITEMS, linebreak, DEFAULT_LIST_DELIMITER);
	}

	public static <T> String displayList(List <T> list) {
		return displayList(list, false);
	}

	/**
	 * display a dense vector
	 *
	 * @param dv
	 * @return
	 */
	public static String displayDenseVector(DenseVector dv) {
		double[] data = dv.getData();
		return display(Arrays.asList(ArrayUtils.toObject(data)));
	}

	/**
	 * display a dense matrix
	 *
	 * @param dm
	 * @return
	 */
	public static String displayDenseMatrix(DenseMatrix dm) {
		StringBuilder sbd = new StringBuilder();
		sbd.append(String.format("mat[%d,%d]:\n", dm.numRows(), dm.numCols()));
		List <DenseVector> list = new ArrayList <>();
		for (int i = 0; i < dm.numRows(); i++) {
			list.add(new DenseVector(dm.getRow(i)));
		}
		sbd.append(displayList(list, 2, true, DEFAULT_LIST_DELIMITER));
		return sbd.toString();
	}

	/**
	 * display a map
	 *
	 * @param m
	 * @param linebreak
	 * @return
	 */
	public static String displayMap(Map <?, ?> m, int numEdgeItems, boolean linebreak) {
		StringBuilder sbd = new StringBuilder();
		String delimiter = linebreak ? ",\n" : ", ";
		ArrayList <Map.Entry <?, ?>> list = new ArrayList <>(m.entrySet());
		if (list.size() <= 2 * numEdgeItems) {
			for (int i = 0; i < list.size(); i += 1) {
				Entry <?, ?> entry = list.get(i);
				sbd.append(display(entry.getKey()));
				sbd.append(": ");
				sbd.append(display(entry.getValue()));
				if (i < m.size() - 1) {
					sbd.append(delimiter);
				}
			}
		} else {
			for (int i = 0; i < numEdgeItems; i += 1) {
				Entry <?, ?> entry = list.get(i);
				sbd.append(display(entry.getKey()));
				sbd.append(": ");
				sbd.append(display(entry.getValue()));
				sbd.append(delimiter);
			}
			sbd.append("...");
			sbd.append(delimiter);
			for (int i = list.size() - numEdgeItems; i < list.size(); i += 1) {
				Entry <?, ?> entry = list.get(i);
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

	public static String displayMap(Map <?, ?> m) {
		return displayMap(m, MAP_NUM_EDGE_ITEMS, true);
	}

	/**
	 * Display 2D array as a table. If the table is very large, you can truncate rows/columns in the middle before
	 * passing it.
	 *
	 * @param table         table data
	 * @param nRows         number of rows
	 * @param nCols         number of columns
	 * @param rowNames      names of rows, nullable
	 * @param colNames      columns of columns, nullable
	 * @param cornerName    name of the top-left corner, nullable
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
		return displayTable(
			table, nRows, nCols, rowNames,
			colNames, cornerName, nRowEdgeItems,
			nRowEdgeItems, nColEdgeItems, useRawDoubleFormat
		);
	}

	public static <T> String displayTable(T[][] table, int nRows, int nCols,
										  String[] rowNames, String[] colNames, String cornerName,
										  int nRowTopEdgeItems, int nRowBottomEdgeItems, int nColEdgeItems,
										  boolean useRawDoubleFormat) {
		if (null == colNames) {
			cornerName = "";
		}
		int[] colWidth = new int[nCols];
		for (int i = 0; i < nCols; i += 1) {
			colWidth[i] = 3;
			if (null != colNames) {
				colWidth[i] = Math.max(colWidth[i], colNames[i].length());
			}
		}

		for (int i = 0; i < nRows; i += 1) {
			if ((i >= nRowTopEdgeItems) && (i < nRows - nRowBottomEdgeItems)) {
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
			if ((i >= nRowTopEdgeItems) && (i < nRows - nRowBottomEdgeItems)) {
				if (i == nRowTopEdgeItems) {
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
	 *
	 * @param table      table data
	 * @param nRows      number of rows
	 * @param nCols      number of columns
	 * @param rowNames   names of rows, nullable
	 * @param colNames   columns of columns, nullable
	 * @param cornerName name of the top-left corner, nullable
	 * @param <T>
	 * @return
	 */
	public static <T> String displayTable(T[][] table, int nRows, int nCols,
										  String[] rowNames, String[] colNames, String cornerName) {
		return displayTable(table, nRows, nCols, rowNames, colNames, cornerName, LIST_NUM_EDGE_ITEMS,
			LIST_NUM_EDGE_ITEMS);
	}

	/**
	 * Split text into multiple lines, prepend spaces, and then join to one string.
	 *
	 * @param text       text using '\n' for linebreaks
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
	 * Split text into multiple lines, prepend s to the first line and spaces to other lines, and then join to one
	 * string. The number of spaces is equal to the length of s
	 *
	 * @param text text
	 * @param s    the string to be prepended in the first line
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

	/**
	 * Display a slice of tensor. The slice is starting from dimension `dim`.
	 *
	 * @param dim        starting dimension of the slice. When is zero, display the original tensor.
	 * @param shape      shape of the original tensor.
	 * @param index      index of the first element in the current slice.
	 * @param valueStrs  strings of all values in the tensor, in row-major order.
	 * @param nEdgeItems the number of edge items. For each dimension, only first and last number of edge items are *
	 *                   displayed, while others are omitted as ellipse.
	 * @return
	 */
	static StringBuilder displayTensorSlice(int dim, long[] shape, int index, String[] valueStrs, int nEdgeItems) {
		StringBuilder sbd = new StringBuilder();
		int dimSize = Math.toIntExact(shape[dim]);
		sbd.append("[");
		if (dim == shape.length - 1) {
			for (int i = 0; i < dimSize; i += 1) {
				if (i > 0) {
					sbd.append(" ");
				}
				if (i >= nEdgeItems && i < dimSize - nEdgeItems) {
					sbd.append("...");
					i = dimSize - nEdgeItems - 1;
				} else {
					sbd.append(valueStrs[index + i]);
				}
			}
		} else {
			int numElemSubSlice = 1;
			for (int i = dim + 1; i < shape.length; i += 1) {
				numElemSubSlice *= shape[i];
			}
			for (int i = 0; i < dimSize; i += 1) {
				if (i > 0) {
					sbd.append("\n");
					sbd.append(StringUtils.repeat(" ", dim + 1));
				}
				if (i >= nEdgeItems && i < dimSize - nEdgeItems) {
					sbd.append("...");
					i = dimSize - nEdgeItems - 1;
				} else {
					sbd.append(displayTensorSlice(dim + 1, shape, index + numElemSubSlice * i,
						valueStrs, nEdgeItems));
				}
			}
		}
		sbd.append("]");
		return sbd;
	}

	/**
	 * Display a tensor in a same format as numpy.array and TensorFlow tensor.
	 *
	 * @param shape      shape of the tensor.
	 * @param valueStrs  strings of all values in the tensor, in row-major order.
	 * @param nEdgeItems the number of edge items. For each dimension, only first and last number of edge items are
	 *                   displayed, while others are omitted as ellipse.
	 * @return
	 */
	public static String displayTensor(long[] shape, String[] valueStrs, int nEdgeItems) {
		return PrettyDisplayUtils.displayTensorSlice(0, shape, 0, valueStrs, nEdgeItems).toString();
	}
}
