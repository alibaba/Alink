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

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.mapper.FlatModelMapper;
import com.alibaba.alink.common.mapper.FlatModelMapperAdapter;
import com.alibaba.alink.common.mapper.IterableModelLoader;
import com.alibaba.alink.common.mapper.IterableModelLoaderFlatModelMapperAdapter;
import com.alibaba.alink.common.model.BroadcastVariableModelSource;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InputPorts(values = {@PortSpec(PortType.MODEL), @PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
@Internal
public class FlatModelMapBatchOp<T extends FlatModelMapBatchOp <T>> extends BatchOperator <T> {

	private static final Logger LOG = LoggerFactory.getLogger(FlatModelMapBatchOp.class);

	private static final String BROADCAST_MODEL_TABLE_NAME = "broadcastModelTable";
	private static final long serialVersionUID = -4974798285811955808L;

	/**
	 * (modelScheme, dataSchema, params) -> FlatModelMapper
	 */
	private final TriFunction <TableSchema, TableSchema, Params, FlatModelMapper> mapperBuilder;

	public FlatModelMapBatchOp(
		TriFunction <TableSchema, TableSchema, Params, FlatModelMapper> mapperBuilder,
		Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		try {
			BroadcastVariableModelSource modelSource = new BroadcastVariableModelSource(BROADCAST_MODEL_TABLE_NAME);
			FlatModelMapper flatMapper = this.mapperBuilder.apply(
				inputs[0].getSchema(),
				inputs[1].getSchema(),
				this.getParams());
			DataSet <Row> resultRows;
			if (flatMapper instanceof IterableModelLoader) {
				final long handler = IterTaskObjKeeper.getNewHandle();
				DataSet <Row> modelRows = inputs[0].getDataSet();
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
							((IterableModelLoader) flatMapper).loadIterableModel(values);
							IterTaskObjKeeper.put(handler, taskId, flatMapper);
						}
					});

				resultRows = inputs[1].getDataSet()
					.flatMap(new IterableModelLoaderFlatModelMapperAdapter(handler))
					.withBroadcastSet(barrier, "barrier");
			} else {
				DataSet <Row> modelRows = inputs[0].getDataSet().rebalance();
				resultRows = inputs[1].getDataSet()
					.flatMap(new FlatModelMapperAdapter(flatMapper, modelSource))
					.withBroadcastSet(modelRows, BROADCAST_MODEL_TABLE_NAME);
			}
			TableSchema outputSchema = flatMapper.getOutputSchema();
			this.setOutput(resultRows, outputSchema);
			return (T) this;
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}
}
