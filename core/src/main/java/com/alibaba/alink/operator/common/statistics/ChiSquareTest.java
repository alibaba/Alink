package com.alibaba.alink.operator.common.statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;

import java.util.HashMap;
import java.util.Map;

/**
 * for chi-square test and chi-square selector.
 */
public class ChiSquareTest {

	/**
	 * @param in:        the last col is label col, others are selectedCols.
	 * @param sessionId: sessionId
	 * @return 3 cols, 1th is colId, 2th is pValue, 3th is chi-square value
	 */
	protected static DataSet <Row> test(DataSet <Row> in, Long sessionId) {
		//flatting data to triple.
		DataSet <Row> dataSet = in
			.flatMap(new FlatMapFunction <Row, Row>() {
				private static final long serialVersionUID = -5007568317570417558L;

				@Override
				public void flatMap(Row row, Collector <Row> result) {
					int n = row.getArity() - 1;
					String nStr = String.valueOf(row.getField(n));
					for (int i = 0; i < n; i++) {
						Row out = new Row(3);
						out.setField(0, i);
						out.setField(1, String.valueOf(row.getField(i)));
						out.setField(2, nStr);
						result.collect(out);
					}
				}
			});

		Table data = DataSetConversionUtil.toTable(
			sessionId,
			dataSet,
			new String[] {"col", "feature", "label"},
			new TypeInformation[] {Types.INT, Types.STRING, Types.STRING});

		//calculate cross table  and chiSquare test.
		return DataSetConversionUtil.fromTable(sessionId, data
			.groupBy("col,feature,label")
			.select("col,feature,label,count(1) as count2"))
			.groupBy("col").reduceGroup(
				new GroupReduceFunction <Row, Tuple2 <Integer, Crosstab>>() {
					private static final long serialVersionUID = 3320220768468472007L;

					@Override
					public void reduce(Iterable <Row> iterable, Collector <Tuple2 <Integer, Crosstab>> collector) {
						Map <Tuple2 <String, String>, Long> map = new HashMap <>();
						int colIdx = -1;
						for (Row row : iterable) {
							map.put(Tuple2.of(row.getField(1).toString(),
								row.getField(2).toString()),
								(long) row.getField(3));
							colIdx = (Integer) row.getField(0);
						}
						collector.collect(new Tuple2 <>(colIdx, Crosstab.convert(map)));
					}
				})
			.map(new ChiSquareTestFromCrossTable());
	}

	/**
	 * @param crossTabWithId: f0 is id, f1 is cross table
	 * @return tuple4: f0 is id which is id of cross table, f1 is pValue, f2 is chi-square Value, f3 is df
	 */
	public static Tuple4 <Integer, Double, Double, Double> test(Tuple2 <Integer, Crosstab> crossTabWithId) {
		int colIdx = crossTabWithId.f0;
		Crosstab crosstab = crossTabWithId.f1;

		int rowLen = crosstab.rowTags.size();
		int colLen = crosstab.colTags.size();

		//compute row sum and col sum
		double[] rowSum = crosstab.rowSum();
		double[] colSum = crosstab.colSum();
		double n = crosstab.sum();

		//compute statistic value
		double chiSq = 0;
		for (int i = 0; i < rowLen; i++) {
			for (int j = 0; j < colLen; j++) {
				double nij = rowSum[i] * colSum[j] / n;
				double temp = crosstab.data[i][j] - nij;
				chiSq += temp * temp / nij;
			}
		}

		//set result
		double p;
		if (rowLen <= 1 || colLen <= 1) {
			p = 1;
		} else {
			ChiSquaredDistribution distribution =
				new ChiSquaredDistribution(null, (rowLen - 1) * (colLen - 1));
			p = 1.0 - distribution.cumulativeProbability(Math.abs(chiSq));
		}

		return Tuple4.of(colIdx, p, chiSq, (double) (rowLen - 1) * (colLen - 1));
	}

	/**
	 * calculate chi-square test value from cross table.
	 */
	static class ChiSquareTestFromCrossTable implements MapFunction <Tuple2 <Integer, Crosstab>, Row> {

		private static final long serialVersionUID = 4588157669356711825L;

		ChiSquareTestFromCrossTable() {
		}

		/**
		 * The mapping method. Takes an element from the input data set and transforms
		 * it into exactly one element.
		 *
		 * @param crossTabWithId The input value.
		 * @return The transformed value
		 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
		 * to fail and may trigger recovery.
		 */
		@Override
		public Row map(Tuple2 <Integer, Crosstab> crossTabWithId) throws Exception {
			Tuple4 tuple4 = ChiSquareTest.test(crossTabWithId);

			Row row = new Row(4);
			row.setField(0, tuple4.f0);
			row.setField(1, tuple4.f1);
			row.setField(2, tuple4.f2);
			row.setField(3, tuple4.f3);

			return row;
		}
	}

}
