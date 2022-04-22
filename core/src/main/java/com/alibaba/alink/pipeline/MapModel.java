package com.alibaba.alink.pipeline;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.io.ModelFileSinkParams;

import java.util.List;

/**
 * Abstract class for a local-predictable Model {@link ModelBase}.
 * <p>
 * A MapModel predict the instance in single input with single-output or multiple-output.
 *
 * @param <T> class type of the {@link MapModel} implementation itself.
 */
public abstract class MapModel<T extends MapModel <T>>
	extends ModelBase <T>
	implements ModelStreamScanParams <T>, LocalPredictable {

	private static final long serialVersionUID = 8333228095437207694L;
	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	protected MapModel(TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
					   Params params) {
		super(params);
		this.mapperBuilder = Preconditions.checkNotNull(mapperBuilder, "mapperBuilder can not be null");
	}

	void validate(TableSchema modelSchema, TableSchema dataSchema) {
		mapperBuilder.apply(modelSchema, dataSchema, params);
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		return postProcessTransformResult(new ModelMapBatchOp <>(this.mapperBuilder, this.params)
			.linkFrom(this.getModelData(), input));
	}

	@Override
	public StreamOperator <?> transform(StreamOperator <?> input) {
		BatchOperator <?> model = this.getModelData();
		return new ModelMapStreamOp <>(model, this.mapperBuilder,
			this.params).linkFrom(input);
	}

	@Override
	public LocalPredictor collectLocalPredictor(TableSchema inputSchema) throws Exception {
		List <Row> modelRows = this.modelData.collect();
		ModelMapper mapper = mapperBuilder.apply(modelData.getSchema(), inputSchema, this.getParams());
		mapper.loadModel(modelRows);
		return new LocalPredictor(mapper);
	}
}
