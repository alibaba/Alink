package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SpearmanCorrelation;
import com.alibaba.alink.params.statistics.VectorCorrelationParams;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Calculating the correlation between two series of data is a common operation in Statistics.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量相关系数")
public final class VectorCorrelationBatchOp extends BatchOperator <VectorCorrelationBatchOp>
	implements VectorCorrelationParams <VectorCorrelationBatchOp> {

	private static final long serialVersionUID = 3325022336197828106L;

	public VectorCorrelationBatchOp() {
		super(null);
	}

	public VectorCorrelationBatchOp(Params params) {
		super(params);
	}

	@Override
	public VectorCorrelationBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String vectorColName = getSelectedCol();

		Method corrType = getMethod();

		if (Method.PEARSON == corrType) {
			DataSet <Tuple2 <BaseVectorSummary, CorrelationResult>> srt = StatisticsHelper.vectorPearsonCorrelation(in,
				vectorColName);

			//block
			DataSet <Row> result = srt
				.flatMap(new FlatMapFunction <Tuple2 <BaseVectorSummary, CorrelationResult>, Row>() {
					private static final long serialVersionUID = 2134644397476490118L;

					@Override
					public void flatMap(Tuple2 <BaseVectorSummary, CorrelationResult> srt, Collector <Row> collector)
						throws Exception {
						new CorrelationDataConverter().save(srt.f1, collector);
					}
				});

			this.setOutput(result, new CorrelationDataConverter().getModelSchema());

		} else {

			DataSet <Row> data = StatisticsHelper.transformToColumns(in, null, vectorColName, null);

			DataSet <Row> rank = SpearmanCorrelation.calcRank(data, true);

			BatchOperator rankOp = new TableSourceBatchOp(DataSetConversionUtil.toTable(getMLEnvironmentId(), rank,
				new String[] {"col"}, new TypeInformation[] {Types.STRING}))
				.setMLEnvironmentId(getMLEnvironmentId());

			VectorCorrelationBatchOp corrBatchOp = new VectorCorrelationBatchOp()
				.setMLEnvironmentId(getMLEnvironmentId())
				.setSelectedCol("col");

			rankOp.link(corrBatchOp);

			this.setOutput(corrBatchOp.getDataSet(), corrBatchOp.getSchema());
		}
		return this;
	}

	public CorrelationResult collectCorrelation() {
		AkPreconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return new CorrelationDataConverter().load(this.collect());
	}

	@SafeVarargs
	public final VectorCorrelationBatchOp lazyCollectCorrelation(Consumer <CorrelationResult>... callbacks) {
		return lazyCollectCorrelation(Arrays.asList(callbacks));
	}

	public final VectorCorrelationBatchOp lazyCollectCorrelation(List <Consumer <CorrelationResult>> callbacks) {
		this.lazyCollect(d -> {
			CorrelationResult correlationResult = new CorrelationDataConverter().load(d);
			for (Consumer <CorrelationResult> callback : callbacks) {
				callback.accept(correlationResult);
			}
		});
		return this;
	}

	public final VectorCorrelationBatchOp lazyPrintCorrelation() {
		return lazyPrintCorrelation(null);
	}

	public final VectorCorrelationBatchOp lazyPrintCorrelation(String title) {
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








