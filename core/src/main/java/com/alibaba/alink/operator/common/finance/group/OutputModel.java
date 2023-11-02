package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Transfer the state to model rows.
 */
public class OutputModel extends CompleteResultFunction {

	@Override
	public List <Row> calc(ComContext context) {
		if (context.getTaskId() != 0) {
			return null;
		}

		List <GroupNode> allNodes = context.getObj(GroupScoreCardVariable.GROUP_ALL_NODES);

		SimpleGroupScoreModelData model = new SimpleGroupScoreModelData();
		model.allNodes = allNodes;

		List <Row> out = new ArrayList <>();
		ListCollector <Row> rows = new ListCollector(out);

		model.labelValues = ((List<Object>)context.getObj(GroupScoreCardVariable.LABEL_VALUES)).toArray();

		new GroupScoreModelConverter().save(model, rows);
		return out;
	}
}
