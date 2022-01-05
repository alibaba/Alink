package com.alibaba.alink.operator.common.regression.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.tensorflow.BaseTFTableModelData;

import java.util.List;

public class TFTableModelRegressionModelData extends BaseTFTableModelData {

	protected String preprocessPipelineModelSchemaStr;
	protected List <Row> preprocessPipelineModelRows;

	public TFTableModelRegressionModelData() {
	}

	public TFTableModelRegressionModelData(Params meta, String[] tfInputCols, Iterable <Row> tfModelRows,
										   String tfOutputSignatureDef, TypeInformation <?> tfOutputSignatureType,
										   String preprocessPipelineModelSchemaStr,
										   List <Row> preprocessPipelineModelRows) {
		super(meta, tfInputCols, tfModelRows, tfOutputSignatureDef, tfOutputSignatureType);
		this.preprocessPipelineModelSchemaStr = preprocessPipelineModelSchemaStr;
		this.preprocessPipelineModelRows = preprocessPipelineModelRows;
	}

	public String getPreprocessPipelineModelSchemaStr() {
		return preprocessPipelineModelSchemaStr;
	}

	public TFTableModelRegressionModelData setPreprocessPipelineModelSchemaStr(
		String preprocessPipelineModelSchemaStr) {
		this.preprocessPipelineModelSchemaStr = preprocessPipelineModelSchemaStr;
		return this;
	}

	public List <Row> getPreprocessPipelineModelRows() {
		return preprocessPipelineModelRows;
	}

	public TFTableModelRegressionModelData setPreprocessPipelineModelRows(
		List <Row> preprocessPipelineModelRows) {
		this.preprocessPipelineModelRows = preprocessPipelineModelRows;
		return this;
	}

}
