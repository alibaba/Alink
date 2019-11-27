package com.alibaba.alink.operator.common.tree.paralleltree;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeSplit extends ComputeFunction {
	private static final Logger LOG = LoggerFactory.getLogger(TreeSplit.class);

	public TreeSplit() {}

	@Override
	public void calc(ComContext context) {
		LOG.info("taskId: {}, split task start", context.getTaskId());

		TreeObj treeObj = context.getObj("treeObj");

		LOG.info("taskId: {}, split start", context.getTaskId());
		try {
			treeObj.bestSplit();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		LOG.info("taskId: {}, split end", context.getTaskId());
	}
}
