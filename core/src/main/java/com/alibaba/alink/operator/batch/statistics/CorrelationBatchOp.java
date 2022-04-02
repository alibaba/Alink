package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SpearmanCorrelation;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.statistics.CorrelationParams;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Calculating the correlation between two series of data is a common operation in Statistics.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("相关系数")
public final class CorrelationBatchOp extends BatchOperator <CorrelationBatchOp>
	implements CorrelationParams <CorrelationBatchOp> {

	private static final long serialVersionUID = 5416573608223150672L;

	public CorrelationBatchOp() {
		super(null);
	}

	public CorrelationBatchOp(Params params) {
		super(params);
	}

	@Override
	public CorrelationBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String[] selectedColNames = this.getParams().get(SELECTED_COLS);

		if (selectedColNames == null) {
			selectedColNames = in.getColNames();
		}

		//check col types must be double or bigint
		TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

		Method corrType = getMethod();

		if (Method.PEARSON == corrType) {

			DataSet <Tuple2 <TableSummary, CorrelationResult>> srt = StatisticsHelper.pearsonCorrelation(in,
				selectedColNames);

			DataSet <Row> result = srt.
				flatMap(new FlatMapFunction <Tuple2 <TableSummary, CorrelationResult>, Row>() {
					private static final long serialVersionUID = -4498296161046449646L;

					@Override
					public void flatMap(Tuple2 <TableSummary, CorrelationResult> summary, Collector <Row> collector) {
						new CorrelationDataConverter().save(summary.f1, collector);
					}
				});

			this.setOutput(result, new CorrelationDataConverter().getModelSchema());

		} else {

			DataSet <Row> data = inputs[0].select(selectedColNames).getDataSet();
			DataSet <Row> rank = SpearmanCorrelation.calcRank(data, false);

			TypeInformation[] colTypes = new TypeInformation[selectedColNames.length];
			for (int i = 0; i < colTypes.length; i++) {
				colTypes[i] = Types.DOUBLE;
			}

			BatchOperator rankOp = new TableSourceBatchOp(
				DataSetConversionUtil.toTable(getMLEnvironmentId(), rank, selectedColNames, colTypes))
				.setMLEnvironmentId(getMLEnvironmentId());

			CorrelationBatchOp corrBatchOp = new CorrelationBatchOp()
				.setMLEnvironmentId(getMLEnvironmentId())
				.setSelectedCols(selectedColNames);

			rankOp.link(corrBatchOp);

			this.setOutput(corrBatchOp.getDataSet(), corrBatchOp.getSchema());

		}

		return this;
	}

	public CorrelationResult collectCorrelation() {
		Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return new CorrelationDataConverter().load(this.collect());
	}

	@SafeVarargs
	public final CorrelationBatchOp lazyCollectCorrelation(Consumer <CorrelationResult>... callbacks) {
		return lazyCollectCorrelation(Arrays.asList(callbacks));
	}

	public final CorrelationBatchOp lazyCollectCorrelation(List <Consumer <CorrelationResult>> callbacks) {
		this.lazyCollect(d -> {
			CorrelationResult correlationResult = new CorrelationDataConverter().load(d);
			for (Consumer <CorrelationResult> callback : callbacks) {
				callback.accept(correlationResult);
			}
		});
		return this;
	}

	public final CorrelationBatchOp lazyPrintCorrelation() {
		return lazyPrintCorrelation(null);
	}

	public final CorrelationBatchOp lazyPrintCorrelation(String title) {
		lazyCollectCorrelation(new Consumer <CorrelationResult>() {
			@Override
			public void accept(CorrelationResult summary) {
				if (title != null) {
					System.out.println(title);
				}
				System.out.println(summary.toString());
			}
		});
		return this;
	}

}








