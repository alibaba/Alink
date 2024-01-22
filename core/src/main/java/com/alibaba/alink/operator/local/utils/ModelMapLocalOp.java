package com.alibaba.alink.operator.local.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamsIgnoredOnWebUI;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithSecondInputSpec;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.AkSourceLocalOp;
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
@ParamsIgnoredOnWebUI(names = "modelFilePath")
@Internal
public class ModelMapLocalOp<T extends ModelMapLocalOp <T>> extends LocalOperator <T> implements HasModelFilePath <T> {

	/**
	 * (modelScheme, dataSchema, params) -> ModelMapper
	 */
	protected final TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder;

	public ModelMapLocalOp(TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder, Params params) {
		super(params);
		this.mapperBuilder = mapperBuilder;
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		checkMinOpSize(1, inputs);

		LocalOperator <?> model = inputs.length == 2 ? inputs[0] : null;
		LocalOperator <?> input = inputs.length == 2 ? inputs[1] : inputs[0];

		if (model == null && getParams().get(HasModelFilePath.MODEL_FILE_PATH) != null) {
			model = new AkSourceLocalOp().setFilePath(getModelFilePath());
		} else if (model == null) {
			throw new AkIllegalOperatorParameterException("One of model or modelFilePath should be set.");
		}

		try {
			final ModelMapper mapper = mapperBuilder.apply(model.getSchema(), input.getSchema(), getParams());
			mapper.loadModel(model.getOutputTable().getRows());
			mapper.open();

			List <Row> output = MapLocalOp.execMapper(input, mapper, getParams());

			this.setOutputTable(new MTable(output, mapper.getOutputSchema()));
			mapper.close();
		} catch (ExceptionWithErrorCode ex) {
			throw ex;
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Error. ", ex);
		}
	}

}
