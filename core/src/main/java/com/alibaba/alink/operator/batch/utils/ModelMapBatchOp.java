package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.IterableModelLoader;
import com.alibaba.alink.common.mapper.IterableModelLoaderModelMapperAdapter;
import com.alibaba.alink.common.mapper.IterableModelLoaderModelMapperAdapterMT;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.mapper.ModelMapperAdapterMT;
import com.alibaba.alink.common.mapper.ModelStreamModelMapperAdapter;
import com.alibaba.alink.common.model.BroadcastVariableModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.stream.model.ModelStreamUtils;
import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.HasModelFilePath;

import java.util.List;

/**
 * *
 */
@InputPorts(values = {
	@PortSpec(value = PortType.MODEL, desc = PortDesc.PREDICT_INPUT_MODEL),
	@PortSpec(value = PortType.DATA, desc = PortDesc.PREDICT_INPUT_DATA)
})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
@Internal
public class ModelMapBatchOp<T extends ModelMapBatchOp <T>> extends BatchOperator <T> implements HasModelFilePath<T> {

	private static final String BROADCAST_MODEL_TABLE_NAME = "broadcastModelTable";
	private static final long serialVersionUID = 3479332090254995273L;

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	protected final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapBatchOp(TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
						   Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		BatchOperator<?> model = inputs[0];
		BatchOperator<?> input = inputs[1];

		if (model == null && getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {
			model = new AkSourceBatchOp()
				.setFilePath(FilePath.deserialize(getParams().get(HasModelFilePath.MODEL_FILE_PATH)))
				.setMLEnvironmentId(getMLEnvironmentId());
		} else if (model == null) {
			throw new IllegalArgumentException("One of model or modelFilePath should be set.");
		}

		try {
			final ModelMapper mapper = mapperBuilder.apply(
				model.getSchema(),
				input.getSchema(),
				getParams()
			);

			DataSet <Row> resultRows = calcResultRows(model, input, mapper, getParams());

			TableSchema outputSchema = mapper.getOutputSchema();
			setOutput(resultRows, outputSchema);
			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static DataSet <Row> calcResultRows(BatchOperator <?> input_model, BatchOperator <?> input_data,
											   ModelMapper mapper, Params params) {
		if (mapper instanceof IterableModelLoader) {
			final long handler = IterTaskObjKeeper.getNewHandle();
			DataSet <Row> modelRows = input_model.getDataSet();
			DataSet <Row> distributedModelRows = modelRows.flatMap(
					new RichFlatMapFunction <Row, Tuple2 <Integer, Row>>() {
						private static final long serialVersionUID = 3544759002096859673L;
						int numTask;

						@Override
						public void open(Configuration parameters) {
							numTask = getRuntimeContext().getNumberOfParallelSubtasks();
						}

						@Override
						public void flatMap(Row value, Collector <Tuple2 <Integer, Row>> out) {
							for (int i = 0; i < numTask; ++i) {
								out.collect(Tuple2.of(i, value));
							}
						}
					}).returns(new TupleTypeInfo <>(Types.INT, modelRows.getType()))
				.partitionCustom(new Partitioner <Integer>() {
					private static final long serialVersionUID = -2924355974935165844L;

					@Override
					public int partition(Integer key, int numPartitions) {
						return key;
					}
				}, 0)
				.map(new MapFunction <Tuple2 <Integer, Row>, Row>() {
					private static final long serialVersionUID = 8884296007768771379L;

					@Override
					public Row map(Tuple2 <Integer, Row> value) throws Exception {
						return value.f1;
					}
				})
				.returns(modelRows.getType());

			DataSet <Integer> barrier = distributedModelRows.mapPartition(
				new RichMapPartitionFunction <Row, Integer>() {
					private static final long serialVersionUID = 2358845952757630826L;

					@Override
					public void mapPartition(Iterable <Row> values, Collector <Integer> out) {
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						((IterableModelLoader) mapper).loadIterableModel(values);
						IterTaskObjKeeper.put(handler, taskId, mapper);
					}
				});

			if (params.get(ModelMapperParams.NUM_THREADS) <= 1) {
				return input_data.getDataSet()
					.map(new IterableModelLoaderModelMapperAdapter(handler))
					.withBroadcastSet(barrier, "barrier");
			} else {
				return input_data.getDataSet()
					.flatMap(new IterableModelLoaderModelMapperAdapterMT(handler,
						params.get(ModelMapperParams.NUM_THREADS)))
					.withBroadcastSet(barrier, "barrier");
			}
		} else {
			final BroadcastVariableModelSource modelSource = new BroadcastVariableModelSource(
				BROADCAST_MODEL_TABLE_NAME);
			DataSet <Row> modelRows = input_model.getDataSet().rebalance();

			if (ModelStreamUtils.useModelStreamFile(params)) {
				return input_data
					.getDataSet()
					.map(new RichMapFunction <Row, Row>() {
						ModelStreamModelMapperAdapter modelStreamModelMapper;

						@Override
						public void open(Configuration parameters) throws Exception {
							super.open(parameters);
							List <Row> modelRows = modelSource.getModelRows(getRuntimeContext());
							mapper.loadModel(modelRows);
							mapper.open();

							modelStreamModelMapper = new ModelStreamModelMapperAdapter(mapper);
						}

						@Override
						public Row map(Row value) throws Exception {
							return modelStreamModelMapper.map(value);
						}
					})
					.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
			} else if (params.get(ModelMapperParams.NUM_THREADS) <= 1) {
				return input_data
					.getDataSet()
					.map(new ModelMapperAdapter(mapper, modelSource))
					.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
			} else {
				return input_data
					.getDataSet()
					.flatMap(
						new ModelMapperAdapterMT(mapper, modelSource,
							params.get(ModelMapperParams.NUM_THREADS))
					)
					.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
			}
		}
	}
}
