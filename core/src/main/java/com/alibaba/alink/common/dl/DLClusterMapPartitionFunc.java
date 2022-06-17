package com.alibaba.alink.common.dl;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;

/**
 * An Flink MapPartition operator to collect IPs and ports and launch python processes.
 */
public class DLClusterMapPartitionFunc extends RichMapPartitionFunction <Row, Row>
	implements ResultTypeQueryable <Row> {
	private final DLFlatMapFunction dlFlatMapFunction;
	private final IpPortFlatMapFunction ipPortFunction;
	private final int numWorkers;
	private final int numPSs;
	private final int numOutputFields;
	private transient int stepNo;

	private static class IpPortCollector implements Collector <Row> {
		private final Collector <Row> opCollector;
		private final int arity;

		public IpPortCollector(Collector <Row> opCollector, int arity) {
			this.opCollector = opCollector;
			this.arity = arity;
		}

		@Override
		public void collect(Row record) {
			Row output = new Row(arity);
			output.setField(arity - 1, record.getField(0));
			opCollector.collect(output);
		}

		@Override
		public void close() {
		}
	}

	private static class TFCollector implements Collector <Row> {
		private final Collector <Row> opCollector;

		public TFCollector(Collector <Row> opCollector) {
			this.opCollector = opCollector;
		}

		@Override
		public void collect(Row record) {
			Row output = new Row(record.getArity() + 1);
			for (int i = 0; i < record.getArity(); i++) {
				output.setField(i, record.getField(i));
			}
			opCollector.collect(output);
		}

		@Override
		public void close() {
		}
	}

	public DLClusterMapPartitionFunc(MLConfig config, TableSchema inputSchema, TableSchema outputSchema,
									 ResourcePluginFactory factory) {
		dlFlatMapFunction = new DLFlatMapFunction(ExecutionMode.TRAIN, config, inputSchema, outputSchema, factory);
		ipPortFunction = new IpPortFlatMapFunction();
		numWorkers = Integer.parseInt(config.getProperties().get(DLConstants.NUM_WORKERS));
		numPSs = Integer.parseInt(config.getProperties().get(DLConstants.NUM_PSS));
		numOutputFields = outputSchema.getFieldNames().length;
	}

	private boolean isDummyTask() {
		return getRuntimeContext().getIndexOfThisSubtask() >= (numWorkers + numPSs);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (isDummyTask()) {
			return;
		}
		stepNo = getIterationRuntimeContext().getSuperstepNumber();
		Preconditions.checkArgument(stepNo <= 2);
		if (stepNo == 1) {
			ipPortFunction.open(getRuntimeContext());
		} else if (stepNo == 2) {
			dlFlatMapFunction.open(getRuntimeContext());
		}
	}

	@Override
	public void close() {
		if (isDummyTask()) {
			return;
		}
		if (stepNo == 1) {
			ipPortFunction.close();
		} else if (stepNo == 2) {
			dlFlatMapFunction.close();
		}
	}

	@Override
	public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
		if (isDummyTask()) {
			values.forEach(t -> {
				// doing nothing, just to avoid a bug in Blink.
			});
			return;
		}
		if (stepNo == 1) {
			ipPortFunction.flatMap(null, new IpPortCollector(out, numOutputFields + 1));
			values.forEach(t -> {
				// doing nothing, just to avoid a bug in Blink.
			});
		} else if (stepNo == 2) {
			Collector <Row> tfCollector = new TFCollector(out);
			for (Row value : values) {
				dlFlatMapFunction.flatMap(value, tfCollector);
			}
		}
	}

	@Override
	public TypeInformation <Row> getProducedType() {
		return dlFlatMapFunction.getProducedType();
	}
}
