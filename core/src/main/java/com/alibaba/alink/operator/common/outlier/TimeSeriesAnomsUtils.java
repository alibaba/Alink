package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.probabilistic.IDF;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.outlier.CalcMidian;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TimeSeriesAnomsUtils {
	public static final String COL_DELIMITER = ",";
	public static final String VAL_DELIMITER = ":";


	/**
	 * return `mad` = median(abs(`series` - center))/`c`  median absolute deviations
	 * where c is the normalization constant.
	 */
	public static double mad(CalcMidian calcMidian) {
		return calcMidian.absMedian(0.6745);
	}

	public static double tppf(double prob, int freedom) {
		return IDF.studentT(prob, freedom);
	}

	public static double calcKSigmaScore(double value, double mean, double variance) {
		if (variance == 0) {
			return -1;
		}
		return Math.pow((value - mean), 2) / variance;
	}

	public static boolean judgeBoxPlotAnom(double value, double q1, double q3, double k) {
		double range = q3 - q1;
		return (value < q1 && ((q1 - value) / range) > k) || (value > q3 && (value - q3) / range > k);
	}

	public static SparseVector generateOutput(List <Integer> indices, double[] data) {
		return generateOutput(indices.stream().mapToInt(a -> a).toArray(), data);
	}

	public static SparseVector generateOutput(int[] indices, double[] data) {
		int itemNum = indices.length;
		int dataLength = data.length;
		double[] values = new double[itemNum];
		for (int i = 0; i < itemNum; i++) {
			values[i] = data[indices[i]];
		}
		return new SparseVector(dataLength, indices, values);
	}

	public static double sumArray(double[] data, int length) {
		double value = 0;
		for (int i = 0; i < length; i++) {
			value += data[i];
		}
		return value;
	}

	public static double mean(double[] data) {
		int length = data.length;
		double sum = sumArray(data, length);
		return sum / length;
	}

	//sort ascend.
	public static class SortTimeSeq implements Comparator <Tuple3 <Comparable, Double, Object[]>> {
		@Override
		public int compare(Tuple3 <Comparable, Double, Object[]> o1, Tuple3 <Comparable, Double, Object[]> o2) {
			return o1.f0.compareTo(o2.f0);
		}
	}

	public static class AddSameGroupId extends RichMapFunction <Row, Row> {

		private static final long serialVersionUID = -6743739645435262920L;

		@Override
		public Row map(Row value) throws Exception {
			int length = value.getArity();
			Row res = new Row(length + 1);
			res.setField(0, 1);
			for (int i = 0; i < length; i++) {
				res.setField(i + 1, value.getField(i));
			}
			return res;
		}
	}

	//this is for time series algo.
	public static Table getStreamTable(long id, DataStream <Row> data,
									   Tuple2 <String[], TypeInformation[]> schema, TypeInformation[] groupTypes) {
		String[] colNames = schema.f0;
		TypeInformation[] colTypes = schema.f1;
		return getStreamTable(id, data, colNames, colTypes, groupTypes);
	}

	//this is for no use graph algo.
	public static Table getStreamTable(long id, DataStream <Row> data,
									   String[] colNames, TypeInformation[] colTypes, TypeInformation[] groupTypes) {
		boolean containsIdCol = colNames[0] != null;
		if (!containsIdCol) {
			colNames[0] = "tempId";
			colTypes[0] = TypeInformation.of(Comparable.class);
		}
		if (groupTypes != null) {
			System.arraycopy(groupTypes, 0, colTypes, 0, groupTypes.length);
		}
		Table table = DataStreamConversionUtil.toTable(id, data, colNames, colTypes);
		if (!containsIdCol) {
			String[] outColNames = Arrays.copyOfRange(colNames, 1, colNames.length);
			table = table.select(join(outColNames, ","));
		}
		return table;
	}

	public static Table getBatchTable(long id, DataSet <Row> data,
									  Tuple2 <String[], TypeInformation[]> schema, TypeInformation[] groupTypes) {
		String[] colNames = schema.f0;
		TypeInformation[] colTypes = schema.f1;
		boolean containsIdCol = groupTypes != null;
		if (!containsIdCol) {
			colNames[0] = "tempId";
			colTypes[0] = TypeInformation.of(Comparable.class);
		} else {
			System.arraycopy(groupTypes, 0, colTypes, 0, groupTypes.length);
		}
		Table table = DataSetConversionUtil.toTable(id, data, colNames, colTypes);
		if (!containsIdCol) {
			String[] outColNames = Arrays.copyOfRange(colNames, 1, colNames.length);
			table = table.select(join(outColNames, ","));
		}
		return table;
	}

	//transform double array data to string.
	public static String transformData(double[] data) {
		StringBuilder sb = new StringBuilder(String.valueOf(data[0]));
		for (int i = 1; i < data.length; i++) {
			sb.append(COL_DELIMITER).append(String.valueOf(data[i]));
		}
		return sb.toString();
	}

	private static String join(String[] stringArray, String on) {

		int length = stringArray.length;
		if (length == 1) {
			return stringArray[0];
		}
		StringBuilder sb = new StringBuilder().append(stringArray[0]);
		for (int i = 1; i < length; i++) {
			sb.append(on).append(stringArray[i]);
		}
		return sb.toString();
	}

	public static Tuple2 <Comparable[], double[]> generateKvData(String strData) {
		Vector vec = VectorUtil.getVector(strData);
		if (vec instanceof SparseVector) {
			SparseVector sv = (SparseVector) vec;
			Comparable[] indices = new Comparable[sv.getIndices().length];
			for (int i = 0; i < sv.getIndices().length; ++i) {
				indices[i] = sv.getIndices()[i];
			}
			return Tuple2.of(indices, sv.getValues());
		} else if (vec instanceof DenseVector) {
			DenseVector dv = (DenseVector) vec;
			int length = dv.size();
			Comparable[] keys = new Comparable[length];
			for (int i = 0; i < length; i++) {
				keys[i] = i;
			}
			return Tuple2.of(keys, dv.getData());
		} else {
			throw new RuntimeException("vector format err: not sparse and dense.");
		}

	}

	//get kv anomalies data from indices and keys.
	public static String getKvData(Comparable[] keys, SparseVector anomalies) {
		if (anomalies.getIndices() == null || anomalies.getIndices().length == 0) {
			return "";
		}
		return getKvData(keys, anomalies.getIndices(), anomalies.getValues());
	}

	public static String getKvData(Comparable[] keys, int[] indices, double[] data) {
		int length = indices.length;
		if (length == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		sb.append(keys[indices[0]] + VAL_DELIMITER + data[0]);
		if (length == 1) {
			return sb.toString();
		} else {
			for (int i = 1; i < length; i++) {
				sb.append(COL_DELIMITER + keys[indices[i]] + VAL_DELIMITER + data[i]);
			}
			return sb.toString();
		}
	}
}
