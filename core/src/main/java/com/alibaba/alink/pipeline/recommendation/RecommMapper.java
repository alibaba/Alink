package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.recommendation.FourFunction;
import com.alibaba.alink.operator.common.recommendation.RecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommType;

import java.util.List;

/**
 * The ModelMapper for {@link BaseRecommender}.
 */
class RecommMapper extends ModelMapper {

	private static final long serialVersionUID = -3353498411027168031L;
	/**
	 * (modelScheme, dataSchema, params, recommType) -> RecommKernel
	 */
	private final RecommKernel recommKernel;

	public RecommMapper(FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder,
						RecommType recommType,
						TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		this.recommKernel = recommKernelBuilder.apply(modelSchema, dataSchema, params, recommType);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		recommKernel.loadModel(modelRows);
	}

	@Override
	public TableSchema getOutputSchema() {
		return recommKernel.getOutputSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return this.recommKernel.recommend(row);
	}

}
