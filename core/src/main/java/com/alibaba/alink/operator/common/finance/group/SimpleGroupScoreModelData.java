package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;

public class SimpleGroupScoreModelData {
	public List <GroupNode> allNodes;
	public int vectorColIdx;
	public Object[] labelValues;
	public TypeInformation <?> labelType;
}
