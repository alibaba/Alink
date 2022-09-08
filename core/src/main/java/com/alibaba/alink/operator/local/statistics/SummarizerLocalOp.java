package com.alibaba.alink.operator.local.statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SummaryDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.local.LocalOperator;
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
public class SummarizerLocalOp extends LocalOperator <SummarizerLocalOp>
	implements SummarizerParams <SummarizerLocalOp> {

	/**
	 * default constructor.
	 */
	public SummarizerLocalOp() {
		super(null);
	}

	/**
	 * constructor with params.
	 */
	public SummarizerLocalOp(Params params) {
		super(params);
	}

	@Override
	public SummarizerLocalOp linkFrom(LocalOperator <?>... inputs) {
		checkOpSize(1, inputs);
		LocalOperator <?> in = inputs[0];

		String[] selectedColNames = in.getColNames();
		if (this.getParams().contains(SummarizerParams.SELECTED_COLS)) {
			selectedColNames = this.getParams().get(SummarizerParams.SELECTED_COLS);
		}

		TableSummary summary=in.getOutputTable().summary(selectedColNames);
		SummaryDataConverter modelConverter = new SummaryDataConverter();
		RowCollector rowCollector=new RowCollector();
		modelConverter.save(summary, rowCollector);

		this.setOutputTable(new MTable(rowCollector.getRows(), modelConverter.getModelSchema()));
		return this;
	}

	public TableSummary collectSummary() {
		AkPreconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return new SummaryDataConverter().load(this.collect());
	}

	public final SummarizerLocalOp lazyCollectSummary(List <Consumer <TableSummary>> callbacks) {
		this.lazyCollect(d -> {
			TableSummary summary = new SummaryDataConverter().load(d);
			for (Consumer <TableSummary> callback : callbacks) {
				callback.accept(summary);
			}
		});
		return this;
	}

	@SafeVarargs
	public final SummarizerLocalOp lazyCollectSummary(Consumer <TableSummary>... callbacks) {
		return lazyCollectSummary(Arrays.asList(callbacks));
	}

	public final SummarizerLocalOp lazyPrintSummary() {
		return lazyPrintSummary(null);
	}

	public final SummarizerLocalOp lazyPrintSummary(String title) {
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
