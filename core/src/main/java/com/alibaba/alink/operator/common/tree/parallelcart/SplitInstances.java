package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.operator.common.tree.Node;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Data;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SplitInstances extends ComputeFunction {
	private final static Logger LOG = LoggerFactory.getLogger(SplitInstances.class);
	private static final long serialVersionUID = -1127287176494832930L;

	@Override
	public void calc(ComContext context) {
		LOG.info("taskId: {}, {} start", context.getTaskId(), SplitInstances.class.getSimpleName());
		BoostingObjs boostingObjs = context.getObj(InitBoostingObjs.BOOSTING_OBJS);
		HistogramBaseTreeObjs tree = context.getObj("tree");
		Node[] best = context.getObj("best");

		final int maxLeaves = boostingObjs.params.get(GbdtTrainParams.MAX_LEAVES);

		int index = 0;
		int size = tree.queue.size();

		int queueNodeSize = 0;
		for (NodeInfoPair item : tree.queue) {
			queueNodeSize++;
			if (item.big != null) {
				queueNodeSize++;
			}
		}

		for (int i = 0; i < size; ++i) {
			NodeInfoPair item = tree.queue.poll();

			assert item != null;

			// remove the root
			queueNodeSize--;
			item.small.node.copy(best[index]);

			if (item.small.node.isLeaf() || (tree.leaves.size() + queueNodeSize + 2) > maxLeaves) {
				item.small.node.makeLeaf();
				item.small.shrinkageMemory();
				tree.leaves.add(item.small);
			} else {
				// split
				tree.queue.add(
					split(item.small, tree.getDynamicSummary(item.small.node), boostingObjs.indices, boostingObjs.data)
				);
				tree.replaceWithActual(item.small.node);
				// add two children.
				queueNodeSize += 2;
			}

			index++;

			if (item.big != null) {
				// remove the root
				queueNodeSize--;
				item.big.node.copy(best[index]);

				if (best[index].isLeaf() || (tree.leaves.size() + queueNodeSize + 2) > maxLeaves) {
					item.big.node.makeLeaf();
					item.big.shrinkageMemory();
					tree.leaves.add(item.big);
				} else {
					// split
					tree.queue.add(
						split(item.big, tree.getDynamicSummary(item.big.node), boostingObjs.indices, boostingObjs.data)
					);
					tree.replaceWithActual(item.big.node);
					// add two children.
					queueNodeSize += 2;
				}

				index++;
			}
		}

		if (tree.queue.isEmpty()) {
			boostingObjs.inWeakLearner = false;
		}
		LOG.info("taskId: {}, {} end", context.getTaskId(), SplitInstances.class.getSimpleName());
	}

	public static NodeInfoPair split(
		NodeInfoPair.NodeInfo nodeInfo,
		EpsilonApproQuantile.WQSummary summary,
		int[] indices,
		Data data) {

		int mid = data.splitInstances(nodeInfo.node, summary, indices, nodeInfo.slice);
		int oobMid = data.splitInstances(nodeInfo.node, summary, indices, nodeInfo.oob);

		nodeInfo.node.setNextNodes(new Node[] {new Node(), new Node()});

		return new NodeInfoPair(
			// left
			new NodeInfoPair.NodeInfo(
				nodeInfo.node.getNextNodes()[0],
				new Slice(nodeInfo.slice.start, mid),
				new Slice(nodeInfo.oob.start, oobMid),
				nodeInfo.depth + 1,
				nodeInfo.baggingFeatures
			),
			// right
			new NodeInfoPair.NodeInfo(
				nodeInfo.node.getNextNodes()[1],
				new Slice(mid, nodeInfo.slice.end),
				new Slice(oobMid, nodeInfo.oob.end),
				nodeInfo.depth + 1,
				null
			)
		);
	}
}
