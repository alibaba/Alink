package com.alibaba.alink.operator.common.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.feature.BasedChisqSelectorParams;
import org.apache.commons.lang3.ArrayUtils;

/**
 * for vector chi-square test and chi-square test.
 */
public class ChiSquareTestUtil {

	/**
	 * chi-square test for vector data.
	 *
	 * @param in:            input
	 * @param vectorColName: vector col name
	 * @param labelColName:  label col name
	 * @return chi-square test result
	 */
	public static DataSet <Row> vectorTest(BatchOperator in,
										   String vectorColName,
										   String labelColName) {
		DataSet <Row> dataSet = StatisticsHelper.transformToColumns(in, null,
			vectorColName, new String[] {labelColName});

		return ChiSquareTest.test(dataSet, in.getMLEnvironmentId());
	}

	/**
	 * chi-square test for table data.
	 *
	 * @param in:               input
	 * @param selectedColNames: selected col names
	 * @param labelColName:     label col name
	 * @return chi-square test result
	 */
	public static DataSet <Row> test(BatchOperator in,
									 String[] selectedColNames,
									 String labelColName) {
		in = in.select(ArrayUtils.add(selectedColNames, labelColName));
		return ChiSquareTest.test(in.getDataSet(), in.getMLEnvironmentId());
	}

	/**
	 * build chi-square test result, it is for table and vector.
	 */
	public static Table buildResult(DataSet <Row> in,
									String[] selectedCols,
									String vectorCol,
									Long sessionId) {

		String[] outColNames = new String[] {"col", "chisquare_test"};
		TypeInformation[] outColTypes = new TypeInformation[] {Types.STRING, Types.STRING};
		return DataSetConversionUtil.toTable(sessionId,
			in.map(new BuildResult(selectedCols)),
			outColNames,
			outColTypes);
	}

	/**
	 * build chi-square test result, it is for table and vector.
	 */
	public static Table buildModelInfoResult(DataSet <Row> in,
											 String[] selectedCols,
											 BasedChisqSelectorParams.SelectorType selectorType,
											 int numTopFeatures,
											 double percentile,
											 double fpr,
											 double fdr,
											 double fwe,
											 Long sessionId) {

		String[] outColNames = new String[] {"col",
			"chisquare_test",
			"selector_type",
			"num_top_features",
			"percentile",
			"fpr",
			"fdr",
			"fwe"};
		TypeInformation[] outColTypes = new TypeInformation[] {Types.STRING,
			Types.STRING,
			Types.STRING,
			Types.STRING,
			Types.STRING,
			Types.STRING,
			Types.STRING,
			Types.STRING};

		return DataSetConversionUtil.toTable(sessionId,
			in.map(new BuildModelInfo(selectedCols,
				selectorType,
				numTopFeatures,
				percentile, fpr, fdr, fwe)),
			outColNames,
			outColTypes);
	}

	/**
	 * chi-square test build result.
	 */
	private static class BuildResult implements MapFunction <Row, Row> {
		private static final long serialVersionUID = 3043216661405231563L;
		private String[] selectedColNames;

		BuildResult(String[] selectedColNames) {
			this.selectedColNames = selectedColNames;
		}

		@Override
		public Row map(Row row) throws Exception {
			int id = (Integer) row.getField(0);
			double p = (double) row.getField(1);
			double value = (double) row.getField(2);
			double df = (double) row.getField(3);

			String colName = selectedColNames != null ? selectedColNames[id] : String.valueOf(id);

			ChiSquareTestResult ctr = new ChiSquareTestResult(df, p, value, colName);

			Row out = new Row(2);
			out.setField(0, colName);

			out.setField(1, JsonConverter.toJson(ctr));

			return out;
		}
	}

	/**
	 * chi-square test build result.
	 */
	private static class BuildModelInfo implements MapFunction <Row, Row> {
		private static final long serialVersionUID = 3043216661405231563L;
		private String[] selectedColNames;
		private BasedChisqSelectorParams.SelectorType selectorType;
		private int numTopFeatures;
		private double percentile;
		private double fpr;
		private double fdr;
		private double fwe;

		BuildModelInfo(String[] selectedColNames,
					   BasedChisqSelectorParams.SelectorType selectorType,
					   int numTopFeatures,
					   double percentile,
					   double fpr,
					   double fdr,
					   double fwe) {
			this.selectedColNames = selectedColNames;
			this.selectorType = selectorType;
			this.numTopFeatures = numTopFeatures;
			this.percentile = percentile;
			this.fpr = fpr;
			this.fdr = fdr;
			this.fwe = fwe;
		}

		@Override
		public Row map(Row row) throws Exception {
			int id = (Integer) row.getField(0);
			double p = (double) row.getField(1);
			double value = (double) row.getField(2);
			double df = (double) row.getField(3);

			String colName = selectedColNames != null ? selectedColNames[id] : String.valueOf(id);

			ChiSquareTestResult ctr = new ChiSquareTestResult(df, p, value, colName);

			Row out = new Row(8);
			out.setField(0, colName);

			out.setField(1, JsonConverter.toJson(ctr));
			out.setField(2, JsonConverter.toJson(selectorType));
			out.setField(3, JsonConverter.toJson(numTopFeatures));
			out.setField(4, JsonConverter.toJson(percentile));
			out.setField(5, JsonConverter.toJson(fpr));
			out.setField(6, JsonConverter.toJson(fdr));
			out.setField(7, JsonConverter.toJson(fwe));
			return out;
		}
	}
}
