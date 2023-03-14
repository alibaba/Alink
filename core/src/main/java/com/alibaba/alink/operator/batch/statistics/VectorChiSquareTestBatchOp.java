package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResults;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.statistics.VectorChiSquareTestParams;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Chi-square test is chi-square independence test.
 * Chi-square independence test is to test whether two factors affect each other. Its zero hypothesis is that the two
 * factors are independent of each other.
 * More information on Chi-squared test: http://en.wikipedia.org/wiki/Chi-squared_test
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "labelCol")
@NameCn("向量卡方检验")
@NameEn("Vector ChiSquare Test")
public final class VectorChiSquareTestBatchOp extends BatchOperator <VectorChiSquareTestBatchOp>
	implements VectorChiSquareTestParams <VectorChiSquareTestBatchOp> {

	private static final long serialVersionUID = 704967640144286515L;

	/**
	 * default constructor
	 */
	public VectorChiSquareTestBatchOp() {
		super(null);
	}

	public VectorChiSquareTestBatchOp(Params params) {
		super(params);
	}

	/**
	 * overwrite linkFrom in BatchOperator
	 *
	 * @param inputs input batch op
	 * @return BatchOperator
	 */
	@Override
	public VectorChiSquareTestBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String selectedColName = getSelectedCol();
		String labelColName = getLabelCol();

		TableUtil.assertSelectedColExist(in.getColNames(), selectedColName);
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName);

		this.setOutputTable(ChiSquareTestUtil.buildResult(
			ChiSquareTestUtil.vectorTest(in, selectedColName, labelColName)
			, null,
			selectedColName,
			getMLEnvironmentId()));

		return this;
	}

	/**
	 * @return ChiSquareTestResult[]
	 */
	public ChiSquareTestResults collectChiSquareTest() {
		AkPreconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return toResult(this.collect());
	}

	@SafeVarargs
	public final VectorChiSquareTestBatchOp lazyCollectChiSquareTest(Consumer <ChiSquareTestResults>... callbacks) {
		return lazyCollectChiSquareTest(Arrays.asList(callbacks));
	}

	public final VectorChiSquareTestBatchOp lazyCollectChiSquareTest(
		List <Consumer <ChiSquareTestResults>> callbacks) {
		this.lazyCollect(d -> {
			ChiSquareTestResults summary = toResult(d);
			for (Consumer <ChiSquareTestResults> callback : callbacks) {
				callback.accept(summary);
			}
		});
		return this;
	}

	public final VectorChiSquareTestBatchOp lazyPrintChiSquareTest() {
		return lazyPrintChiSquareTest(null);
	}

	public final VectorChiSquareTestBatchOp lazyPrintChiSquareTest(String title) {
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
		ChiSquareTestResult[] result = new ChiSquareTestResult[rows.size()];

		for (Row row : rows) {
			int id = Integer.parseInt(String.valueOf(row.getField(0)));
			result[id] = JsonConverter.fromJson((String) row.getField(1), ChiSquareTestResult.class);
		}

		ChiSquareTestResults chiSquareTestResults = new ChiSquareTestResults();
		chiSquareTestResults.results = result;
		chiSquareTestResults.selectedCols = new String[result.length];
		for(int i=0; i<chiSquareTestResults.selectedCols.length; i++) {
			chiSquareTestResults.selectedCols[i] = String.valueOf(i);
		}
		return chiSquareTestResults;
	}

}
