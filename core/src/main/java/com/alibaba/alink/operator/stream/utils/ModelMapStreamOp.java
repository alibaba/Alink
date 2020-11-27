package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.directreader.DataBridge;
import com.alibaba.alink.common.io.directreader.DirectReader;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.mapper.ModelMapperAdapterMT;
import com.alibaba.alink.common.model.DataBridgeModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.shared.HasNumThreads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * *
 */
public class ModelMapStreamOp<T extends ModelMapStreamOp <T>> extends StreamOperator <T> {

	private static final long serialVersionUID = -6591412871091394859L;
	private final BatchOperator model;

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	private final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapStreamOp(BatchOperator model,
							TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
							Params params) {

		super(params);
		this.model = model;
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator in = checkAndGetFirst(inputs);
		TableSchema modelSchema = this.model.getSchema();
		try {
			DataBridge modelDataBridge = DirectReader.collect(model);
			DataBridgeModelSource modelSource = new DataBridgeModelSource(modelDataBridge);
			ModelMapper mapper = this.mapperBuilder.apply(modelSchema, in.getSchema(), this.getParams());
			DataStream <Row> resultRows;

			final boolean isStreamPredictMultiThread = AlinkGlobalConfiguration.isStreamPredictMultiThread();

			if (!isStreamPredictMultiThread
				|| !getParams().contains(HasNumThreads.NUM_THREADS)
				|| getParams().get(HasNumThreads.NUM_THREADS) <= 1) {
				resultRows = in
					.getDataStream()
					.map(new ModelMapperAdapter(mapper, modelSource));
			} else {
				resultRows = in
					.getDataStream()
					.flatMap(
						new ModelMapperAdapterMT(mapper, modelSource, getParams().get(HasNumThreads.NUM_THREADS))
					);
			}
			TableSchema resultSchema = mapper.getOutputSchema();
			this.setOutput(resultRows, resultSchema);

			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

	}

	public static class CollectModelRows implements FlatMapFunction <Row, List <Row>> {

		private static final long serialVersionUID = -3066852386865640351L;
		private Map <Long, List <Row>> buffers = new HashMap <>(0);

		@Override
		public void flatMap(Row inRow, Collector <List <Row>> out) throws Exception {
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
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("collect model : " + id);
				}
				out.collect(buffers.get(id));
				buffers.get(id).clear();
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

	public static class PredictProcess extends RichCoFlatMapFunction <Row, List <Row>, Row> {

		private static final long serialVersionUID = -2297080341480913249L;
		private ModelMapper mapper = null;
		private final String[] dataFieldNames;
		private final DataType[] dataFieldTypes;
		private final String[] modelFieldNames;
		private final DataType[] modelFieldTypes;
		private int iter = 0;
		private DataBridge dataBridge;

		public PredictProcess(ModelMapper mapper, TableSchema modelSchema, TableSchema dataSchema,
							  DataBridge dataBridge) {
			this.mapper = mapper;
			this.dataBridge = dataBridge;
			this.dataFieldNames = dataSchema.getFieldNames();
			this.dataFieldTypes = dataSchema.getFieldDataTypes();
			this.modelFieldNames = modelSchema.getFieldNames();
			this.modelFieldTypes = modelSchema.getFieldDataTypes();
		}

		protected TableSchema getDataSchema() {
			return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
		}

		protected TableSchema getModelSchema() {
			return TableSchema.builder().fields(modelFieldNames, modelFieldTypes).build();
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			if (dataBridge != null) {
				// read init model
				List <Row> modelRows = DirectReader.directRead(dataBridge);
				this.mapper.loadModel(modelRows);
			}
		}

		@Override
		public void flatMap1(Row row, Collector <Row> collector) throws Exception {
			collector.collect(this.mapper.map(row));
		}

		@Override
		public void flatMap2(List <Row> modelRows, Collector <Row> collector) throws Exception {
			this.mapper.loadModel(modelRows);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " load model : " + iter++);
			}
		}
	}

}
