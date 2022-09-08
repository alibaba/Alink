package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResults;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.statistics.ChiSquareTestParams;
import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Chi-square test is chi-square independence test.
 * Chi-square independence test is to test whether two factors affect each other.
 * Its zero hypothesis is that the two factors are independent of each other.
 * More information on chi-square test: http://en.wikipedia.org/wiki/Chi-squared_test
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCols")
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("卡方检验")
public final class ChiSquareTestBatchOp extends BatchOperator <ChiSquareTestBatchOp>
	implements ChiSquareTestParams <ChiSquareTestBatchOp> {

	private static final long serialVersionUID = 8407644021306410753L;

	/**
	 * default constructor
	 */
	public ChiSquareTestBatchOp() {
		super(null);
	}

	public ChiSquareTestBatchOp(Params params) {
		super(params);
	}

	/**
	 * overwrite linkFrom in BatchOperator
	 *
	 * @param inputs input batch op
	 * @return BatchOperator
	 */
	@Override
	public ChiSquareTestBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] selectedColNames = getSelectedCols();
		String labelColName = getLabelCol();

		AkPreconditions.checkArgument(!ArrayUtils.isEmpty(selectedColNames),
			"selectedColNames must be set.");

		TableUtil.assertSelectedColExist(in.getColNames(), selectedColNames);
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName);

		this.setOutputTable(ChiSquareTestUtil.buildResult(
			ChiSquareTestUtil.test(in, selectedColNames, labelColName),
			selectedColNames,
			null,
			getMLEnvironmentId()));

		return this;
	}

	public ChiSquareTestResults collectChiSquareTest() {
		AkPreconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return toResult(this.collect());
	}

	@SafeVarargs
	public final ChiSquareTestBatchOp lazyCollectChiSquareTest(Consumer <ChiSquareTestResults>... callbacks) {
		return lazyCollectChiSquareTest(Arrays.asList(callbacks));
	}

	public final ChiSquareTestBatchOp lazyCollectChiSquareTest(List <Consumer <ChiSquareTestResults>> callbacks) {
		this.lazyCollect(d -> {
			ChiSquareTestResults summary = toResult(d);
			for (Consumer <ChiSquareTestResults> callback : callbacks) {
				callback.accept(summary);
			}
		});
		return this;
	}

	public final ChiSquareTestBatchOp lazyPrintChiSquareTest() {
		return lazyPrintChiSquareTest(null);
	}

	public final ChiSquareTestBatchOp lazyPrintChiSquareTest(String title) {
		lazyCollectChiSquareTest(new Consumer <ChiSquareTestResults>() {
			@Override
			public void accept(ChiSquareTestResults summary) {
				if (title != null) {
					System.out.println(title);
				}

				System.out.println(summary.toString());
			}
		});
		return this;
	}

	private ChiSquareTestResults toResult(List <Row> rows) {
		//get result
		ChiSquareTestResult[] result = new ChiSquareTestResult[rows.size()];
		String[] selectedColNames = getSelectedCols();

		for (Row row : rows) {
			String colName = (String) row.getField(0);

			TableUtil.assertSelectedColExist(selectedColNames, colName);

			result[TableUtil.findColIndexWithAssertAndHint(selectedColNames, colName)] =
				JsonConverter.fromJson((String) row.getField(1), ChiSquareTestResult.class);
		}

		ChiSquareTestResults chiSquareTestResults = new ChiSquareTestResults();
		chiSquareTestResults.results = result;
		chiSquareTestResults.selectedCols = getSelectedCols();
		return chiSquareTestResults;
	}

}
