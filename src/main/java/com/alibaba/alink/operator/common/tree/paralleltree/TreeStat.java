package com.alibaba.alink.operator.common.tree.paralleltree;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

public class TreeStat extends ComputeFunction {
	public TreeStat() {
	}

	@Override
	public void calc(ComContext context) {
		TreeObj treeObj = context.getObj("treeObj");

		treeObj.determineLoopNode();

		treeObj.initialLoop();

		treeObj.stat();

		context.putObj("allReduceCnt", treeObj.histLen());
	}
}
