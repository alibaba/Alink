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
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SummaryDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.statistics.SummarizerParams;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * It is summary of table, support count, mean, variance, min, max, sum.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("全表统计")
public class SummarizerBatchOp extends BatchOperator <SummarizerBatchOp>
	implements SummarizerParams <SummarizerBatchOp> {

	private static final long serialVersionUID = -6352411074245846994L;

	/**
	 * default constructor.
	 */
	public SummarizerBatchOp() {
		super(null);
	}

	/**
	 * constructor with params.
	 */
	public SummarizerBatchOp(Params params) {
		super(params);
	}

	@Override
	public SummarizerBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(1, inputs);
		BatchOperator <?> in = inputs[0];

		String[] selectedColNames = in.getColNames();
		if (this.getParams().contains(SummarizerParams.SELECTED_COLS)) {
			selectedColNames = this.getParams().get(SummarizerParams.SELECTED_COLS);
		}

		DataSet <TableSummary> srt = StatisticsHelper.summary(in, selectedColNames);

		//result may result.
		DataSet <Row> out = srt
			.flatMap(new TableSummaryBuildModel());

		SummaryDataConverter converter = new SummaryDataConverter();

		this.setOutput(out, converter.getModelSchema());

		return this;
	}

	/**
	 * table summary build model.
	 */
	public static class TableSummaryBuildModel implements FlatMapFunction <TableSummary, Row> {

		private static final long serialVersionUID = 6182446910256382298L;

		TableSummaryBuildModel() {

		}

		@Override
		public void flatMap(TableSummary srt, Collector <Row> collector) throws Exception {
			if (null != srt) {
				SummaryDataConverter modelConverter = new SummaryDataConverter();

				modelConverter.save(srt, collector);
			}
		}
	}

	public TableSummary collectSummary() {
		Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return new SummaryDataConverter().load(this.collect());
	}

	public final SummarizerBatchOp lazyCollectSummary(List <Consumer <TableSummary>> callbacks) {
		this.lazyCollect(d -> {
			TableSummary summary = new SummaryDataConverter().load(d);
			for (Consumer <TableSummary> callback : callbacks) {
				callback.accept(summary);
			}
		});
		return this;
	}

	@SafeVarargs
	public final SummarizerBatchOp lazyCollectSummary(Consumer <TableSummary>... callbacks) {
		return lazyCollectSummary(Arrays.asList(callbacks));
	}

	public final SummarizerBatchOp lazyPrintSummary() {
		return lazyPrintSummary(null);
	}

	public final SummarizerBatchOp lazyPrintSummary(String title) {
		lazyCollectSummary(new Consumer <TableSummary>() {
			@Override
			public void accept(TableSummary summary) {
				if (title != null) {
					System.out.println(title);
				}

				System.out.println(summary.toString());
			}
		});
		return this;
	}

}
