package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.alink.operator.common.tree.parallelcart.InitBoostingObjs.BOOSTING_OBJS;

public class InitTreeObjs extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(InitTreeObjs.class);
	private static final long serialVersionUID = -5025775880607091982L;

	public static final String QUANTILE_MODEL = "quantileModel";
	public static final String TREE = "tree";

	@Override
	public void calc(ComContext context) {
		if (context.getStepNo() != 1) {
			return;
		}

		LOG.info("taskId: {}, {} start", context.getTaskId(), InitTreeObjs.class.getSimpleName());
		HistogramBaseTreeObjs tree = new HistogramBaseTreeObjs();

		tree.init(context.getObj(BOOSTING_OBJS), context.getObj(QUANTILE_MODEL));

		context.putObj(TREE, tree);
		LOG.info("taskId: {}, {} end", context.getTaskId(), InitTreeObjs.class.getSimpleName());
	}
}
