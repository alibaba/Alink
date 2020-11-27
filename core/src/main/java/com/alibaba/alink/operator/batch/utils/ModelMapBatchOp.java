package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.mapper.IterableModelLoader;
import com.alibaba.alink.common.mapper.IterableModelLoaderModelMapperAdapter;
import com.alibaba.alink.common.mapper.IterableModelLoaderModelMapperAdapterMT;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.ModelMapperAdapter;
import com.alibaba.alink.common.mapper.ModelMapperAdapterMT;
import com.alibaba.alink.common.model.BroadcastVariableModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.shared.HasNumThreads;

/**
 * *
 */
public class ModelMapBatchOp<T extends ModelMapBatchOp <T>> extends BatchOperator <T> {

	private static final String BROADCAST_MODEL_TABLE_NAME = "broadcastModelTable";
	private static final long serialVersionUID = 3479332090254995273L;

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	private final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapBatchOp(TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
						   Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);
		try {
			ModelMapper mapper = this.mapperBuilder.apply(
				inputs[0].getSchema(),
				inputs[1].getSchema(),
				this.getParams());
			DataSet<Row> resultRows = null;
			if(mapper instanceof IterableModelLoader){
				final long handler = IterTaskObjKeeper.getNewHandle();
				DataSet <Row> modelRows = inputs[0].getDataSet();
				DataSet <Row> distributedModelRows = modelRows.flatMap(
					new RichFlatMapFunction <Row, Tuple2 <Integer, Row>>() {
						private static final long serialVersionUID = 3544759002096859673L;
						int numTask;
						@Override
						public void open(Configuration parameters){
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
					.map(new MapFunction <Tuple2<Integer, Row>, Row>() {
						private static final long serialVersionUID = 8884296007768771379L;

						@Override
						public Row map(Tuple2 <Integer, Row> value) throws Exception {
							return value.f1;
						}
					})
					.returns(modelRows.getType());

				DataSet<Integer> barrier = distributedModelRows.mapPartition(new RichMapPartitionFunction <Row, Integer>() {
					private static final long serialVersionUID = 2358845952757630826L;
					@Override
					public void mapPartition(Iterable <Row> values, Collector <Integer> out) {
						int taskId = getRuntimeContext().getIndexOfThisSubtask();
						((IterableModelLoader) mapper).loadIterableModel(values);
						IterTaskObjKeeper.put(handler, taskId, mapper);
					}
				});

				final boolean isBatchPredictMultiThread = AlinkGlobalConfiguration.isBatchPredictMultiThread();

				if (!isBatchPredictMultiThread
					|| !getParams().contains(HasNumThreads.NUM_THREADS)
					|| getParams().get(HasNumThreads.NUM_THREADS) <= 1) {
					resultRows = inputs[1].getDataSet()
						.map(new IterableModelLoaderModelMapperAdapter(handler))
						.withBroadcastSet(barrier, "barrier");
				} else {
					resultRows = inputs[1].getDataSet()
						.flatMap(new IterableModelLoaderModelMapperAdapterMT(handler, getParams().get(HasNumThreads.NUM_THREADS)))
						.withBroadcastSet(barrier, "barrier");
				}
			}
			else {
				BroadcastVariableModelSource modelSource = new BroadcastVariableModelSource(BROADCAST_MODEL_TABLE_NAME);
				DataSet <Row> modelRows = inputs[0].getDataSet().rebalance();
				final boolean isBatchPredictMultiThread = AlinkGlobalConfiguration.isBatchPredictMultiThread();

				if (!isBatchPredictMultiThread
					|| !getParams().contains(HasNumThreads.NUM_THREADS)
					|| getParams().get(HasNumThreads.NUM_THREADS) <= 1) {
					resultRows = inputs[1]
						.getDataSet()
						.map(new ModelMapperAdapter(mapper, modelSource))
						.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
				} else {
					resultRows = inputs[1]
						.getDataSet()
						.flatMap(
							new ModelMapperAdapterMT(mapper, modelSource, getParams().get(HasNumThreads.NUM_THREADS))
						)
						.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
				}
			}

			TableSchema outputSchema = mapper.getOutputSchema();
			this.setOutput(resultRows, outputSchema);
			return (T) this;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
