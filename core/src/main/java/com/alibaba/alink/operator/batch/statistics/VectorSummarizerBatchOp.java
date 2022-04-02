package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
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
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.VectorSummaryDataConverter;
import com.alibaba.alink.params.statistics.VectorSummarizerParams;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * It is summary of table, support count, mean, variance, min, max, sum.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量全表统计")
public class VectorSummarizerBatchOp extends BatchOperator <VectorSummarizerBatchOp>
	implements VectorSummarizerParams <VectorSummarizerBatchOp> {

	private static final long serialVersionUID = -6710626320480287950L;

	public VectorSummarizerBatchOp() {
		super(null);
	}

	public VectorSummarizerBatchOp(Params params) {
		super(params);
	}

	@Override
	public VectorSummarizerBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		DataSet <BaseVectorSummary> srt = StatisticsHelper.vectorSummary(in, getSelectedCol());

		DataSet <Row> out = srt
			.flatMap(new VectorSummaryBuildModel());

		VectorSummaryDataConverter converter = new VectorSummaryDataConverter();

		this.setOutput(out, converter.getModelSchema());

		return this;
	}

	/**
	 * vector summary build model.
	 */
	public static class VectorSummaryBuildModel implements FlatMapFunction <BaseVectorSummary, Row> {

		private static final long serialVersionUID = -4226261544526437512L;

		VectorSummaryBuildModel() {
		}

		@Override
		public void flatMap(BaseVectorSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				VectorSummaryDataConverter modelConverter = new VectorSummaryDataConverter();
				modelConverter.save(srt, collector);
			}
		}
	}

	public BaseVectorSummary collectVectorSummary() {
		Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return new VectorSummaryDataConverter().load(this.collect());
	}

	@SafeVarargs
	public final VectorSummarizerBatchOp lazyCollectVectorSummary(Consumer <BaseVectorSummary>... callbacks) {
		return lazyCollectVectorSummary(Arrays.asList(callbacks));
	}

	public final VectorSummarizerBatchOp lazyCollectVectorSummary(List <Consumer <BaseVectorSummary>> callbacks) {
		this.lazyCollect(d -> {
			BaseVectorSummary summary = new VectorSummaryDataConverter().load(d);
			for (Consumer <BaseVectorSummary> callback : callbacks) {
				callback.accept(summary);
			}
		});
		return this;
	}

	public final VectorSummarizerBatchOp lazyPrintVectorSummary() {
		return lazyPrintVectorSummary(null);
	}

	public final VectorSummarizerBatchOp lazyPrintVectorSummary(String title) {
		lazyCollectVectorSummary(new Consumer <BaseVectorSummary>() {
			@Override
			public void accept(BaseVectorSummary summary) {
				if (title != null) {
					System.out.println(title);
				}
				System.out.println(summary.toString());
			}
		});
		return this;
	}

}
