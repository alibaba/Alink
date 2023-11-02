package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.model.LabeledModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * summary data converter.
 */
public class GroupScoreModelConverter
	extends LabeledModelDataConverter <SimpleGroupScoreModelData, SimpleGroupScoreModelData> {

	public GroupScoreModelConverter() {
	}

	public GroupScoreModelConverter(TypeInformation labelType) {
		super(labelType);
	}

	@Override
	public Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeModel(SimpleGroupScoreModelData allNodes) {
		List <String> model = new ArrayList <>();
		for (int i = 0; i < allNodes.allNodes.size(); i++) {
			model.add(allNodes.allNodes.get(i).toJson());
		}
		Params params = new Params();

		return Tuple3.of(params, model, Arrays.asList(allNodes.labelValues));
	}

	@Override
	public SimpleGroupScoreModelData deserializeModel(Params meta, Iterable <String> data, Iterable <Object> distinctLabels) {
		SimpleGroupScoreModelData model = new SimpleGroupScoreModelData();
		model.allNodes = new ArrayList <>();
		for (String str : data) {
			model.allNodes.add(JsonConverter.fromJson(str, GroupNode.class));
		}

		if (distinctLabels != null) {
			List <Object> labelList = new ArrayList <>();
			distinctLabels.forEach(labelList::add);
			model.labelValues = labelList.toArray();
		}
		return model;
	}

}
