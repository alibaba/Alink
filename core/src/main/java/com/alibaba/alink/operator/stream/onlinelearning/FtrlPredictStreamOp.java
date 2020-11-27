package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.FtrlPredictParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ftrl predictor receive two stream : model stream and data stream. It using updated model by model stream real-time,
 * and using the newest model predict data stream.
 */
public final class FtrlPredictStreamOp extends StreamOperator <FtrlPredictStreamOp>
	implements FtrlPredictParams <FtrlPredictStreamOp> {

	private static final long serialVersionUID = 1101312461028410493L;
	DataBridge dataBridge = null;

	public FtrlPredictStreamOp(BatchOperator model) {
		super(new Params());
		if (model != null) {
			dataBridge = DirectReader.collect(model);
		} else {
			throw new IllegalArgumentException("Ftrl algo: initial model is null. Please set a valid initial model.");
		}
	}

	public FtrlPredictStreamOp(BatchOperator model, Params params) {
		super(params);
		if (model != null) {
			dataBridge = DirectReader.collect(model);
		} else {
			throw new IllegalArgumentException("Ftrl algo: initial model is null. Please set a valid initial model.");
		}
	}

	@Override
	public FtrlPredictStreamOp linkFrom(StreamOperator <?>... inputs) {
		checkOpSize(2, inputs);

		try {
			DataStream <LinearModelData> modelstr = inputs[0].getDataStream()
				.flatMap(new RichFlatMapFunction <Row, Tuple2 <Integer, Row>>() {
					private static final long serialVersionUID = 6421400378693673120L;

					@Override
					public void flatMap(Row row, Collector <Tuple2 <Integer, Row>> out)
						throws Exception {
						int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
						for (int i = 0; i < numTasks; ++i) {
							out.collect(Tuple2.of(i, row));
						}
					}
				}).partitionCustom(new Partitioner <Integer>() {
					private static final long serialVersionUID = -8529141174150716544L;

					@Override
					public int partition(Integer key, int numPartitions) { return key; }
				}, 0).map(new MapFunction <Tuple2 <Integer, Row>, Row>() {
					private static final long serialVersionUID = -3419050212685018765L;

					@Override
					public Row map(Tuple2 <Integer, Row> value) throws Exception {
						return value.f1;
					}
				})
				.flatMap(new CollectModel());

			TypeInformation[] types = new TypeInformation[3];
			String[] names = new String[3];
			for (int i = 0; i < 3; ++i) {
				names[i] = inputs[0].getSchema().getFieldNames()[i + 2];
				types[i] = inputs[0].getSchema().getFieldTypes()[i + 2];
			}
			TableSchema modelSchema = new TableSchema(names, types);
			/* predict samples */
			DataStream <Row> prediction = inputs[1].getDataStream()
				.connect(modelstr)
				.flatMap(new PredictProcess(modelSchema, inputs[1].getSchema(), this.getParams(), dataBridge));

			this.setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), prediction,
				new LinearModelMapper(modelSchema, inputs[1].getSchema(), getParams()).getOutputSchema()));

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException(ex.toString());
		}

		return this;
	}

	public static class CollectModel implements FlatMapFunction <Row, LinearModelData> {

		private static final long serialVersionUID = -3268487523795502651L;
		private Map <Long, List <Row>> buffers = new HashMap <>(0);

		@Override
		public void flatMap(Row inRow, Collector <LinearModelData> out) throws Exception {
			long id = (long) inRow.getField(0);
			Long nTab = (long) inRow.getField(1);

			Row row = new Row(inRow.getArity() - 2);

			for (int i = 0; i < row.getArity(); ++i) {
				row.setField(i, inRow.getField(i + 2));
			}

			if (buffers.containsKey(id) && buffers.get(id).size() == nTab.intValue() - 1) {

				if (buffers.containsKey(id)) {
					buffers.get(id).add(row);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(row);
					buffers.put(id, buffer);
				}

				LinearModelData ret = new LinearModelDataConverter().load(buffers.get(id));
				buffers.get(id).clear();
				out.collect(ret);
			} else {
				if (buffers.containsKey(id)) {
					buffers.get(id).add(row);
				} else {
					List <Row> buffer = new ArrayList <>(0);
					buffer.add(row);
					buffers.put(id, buffer);
				}
			}
		}
	}

	public static class PredictProcess extends RichCoFlatMapFunction <Row, LinearModelData, Row> {

		private static final long serialVersionUID = 2109057656223474165L;
		private LinearModelMapper predictor = null;
		private final String[] dataFieldNames;
		private final DataType[] dataFieldTypes;
		private final String[] modelFieldNames;
		private final DataType[] modelFieldTypes;
		private Params params;
		private int iter = 0;
		private DataBridge dataBridge;

		public PredictProcess(TableSchema modelSchema, TableSchema dataSchema, Params params, DataBridge dataBridge) {
			this.dataBridge = dataBridge;
			this.dataFieldNames = dataSchema.getFieldNames();
			this.dataFieldTypes = dataSchema.getFieldDataTypes();
			this.modelFieldNames = modelSchema.getFieldNames();
			this.modelFieldTypes = modelSchema.getFieldDataTypes();
			this.params = params;
		}

		protected TableSchema getDataSchema() {
			return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
		}

		protected TableSchema getModelSchema() {
			return TableSchema.builder().fields(modelFieldNames, modelFieldTypes).build();
		}

		@Override
		public void open(Configuration parameters) throws Exception {

			this.predictor = new LinearModelMapper(getModelSchema(), getDataSchema(), this.params);
			if (dataBridge != null) {
				// read init model
				List <Row> modelRows = DirectReader.directRead(dataBridge);
				LinearModelData model = new LinearModelDataConverter().load(modelRows);
				this.predictor.loadModel(model);
			}
		}

		@Override
		public void flatMap1(Row row, Collector <Row> collector) throws Exception {
			collector.collect(this.predictor.map(row));
		}

		@Override
		public void flatMap2(LinearModelData linearModel, Collector <Row> collector) throws Exception {
			this.predictor.loadModel(linearModel);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("taskId: " + getRuntimeContext().getIndexOfThisSubtask() + " load model : " + iter);
			}
			iter++;
		}
	}
}
