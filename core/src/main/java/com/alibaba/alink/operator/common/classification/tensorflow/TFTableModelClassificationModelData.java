package com.alibaba.alink.operator.common.classification.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelData;

import java.util.ArrayList;
import java.util.List;

public class TFTableModelClassificationModelData extends TFTableModelRegressionModelData {

	protected List <Object> sortedLabels;

	public TFTableModelClassificationModelData() {
	}

	public TFTableModelClassificationModelData(Params params, String[] tfInputCols, List <Row> tfModelRows,
											   String tfOutputSignatureDef, TypeInformation <?> tfOutputSignatureType,
											   String preprocessPipelineModelSchemaStr,
											   List <Row> preprocessPipelineModelRows,
											   List <Object> sortedLabels) {
		super(params, tfInputCols, tfModelRows, tfOutputSignatureDef, tfOutputSignatureType,
			preprocessPipelineModelSchemaStr, preprocessPipelineModelRows);
		this.sortedLabels = sortedLabels;
	}

	public List <Object> getSortedLabels() {
		return sortedLabels;
	}

	public TFTableModelClassificationModelData setSortedLabels(Iterable <Object> sortedLabels) {
		this.sortedLabels = new ArrayList <>();
		for (Object sortedLabel : sortedLabels) {
			this.sortedLabels.add(sortedLabel);
		}
		return this;
	}
}
