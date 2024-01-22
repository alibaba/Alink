package com.alibaba.alink.operator.local.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.operator.common.recommendation.FourFunction;
import com.alibaba.alink.operator.common.recommendation.RecommKernel;
import com.alibaba.alink.operator.common.recommendation.RecommMapper;
import com.alibaba.alink.operator.common.recommendation.RecommType;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.AkSourceLocalOp;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.shared.HasModelFilePath;

import java.util.List;

/**
 * *
 */

@Internal
@InputPorts(values = {@PortSpec(PortType.MODEL), @PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ReservedColsWithSecondInputSpec
public class BaseRecommLocalOp<T extends BaseRecommLocalOp <T>> extends LocalOperator <T>
	implements HasModelFilePath <T> {

	/**
	 * (modelScheme, dataSchema, params) -> RecommKernel
	 */
	private final FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder;
	private final RecommType recommType;

	public BaseRecommLocalOp(
		FourFunction <TableSchema, TableSchema, Params, RecommType, RecommKernel> recommKernelBuilder,
		RecommType recommType,
		Params params) {

		super(params);
		this.recommKernelBuilder = recommKernelBuilder;
		this.recommType = recommType;
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		LocalOperator <?> model = inputs.length == 2 ? inputs[0] : null;
		LocalOperator <?> input = inputs.length == 2 ? inputs[1] : inputs[0];

		if (null == model) {
			if (getModelFilePath() != null) {
				model = new AkSourceLocalOp().setFilePath(getModelFilePath());
			} else {
				throw new AkIllegalOperatorParameterException("One of model or modelFilePath should be set.");
			}
		}

		final RecommMapper mapper =
			new RecommMapper(
				this.recommKernelBuilder, this.recommType,
				inputs[0].getSchema(),
				inputs[1].getSchema(), this.getParams()
			);
		mapper.loadModel(model.getOutputTable().getRows());
		mapper.open();

		List <Row> output = MapLocalOp.execMapper(input, mapper, getParams());

		this.setOutputTable(new MTable(output, mapper.getOutputSchema()));
		mapper.close();
	}
}
