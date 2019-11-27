package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class UpdateTreeData extends ComputeFunction {
	private static final Logger LOG = LoggerFactory.getLogger(UpdateTreeData.class);
	private int treeDepth;
	private int treeNum;

	public UpdateTreeData(int treeDepth, int treeNum) {
		this.treeDepth = treeDepth;
		this.treeNum = treeNum;
	}

	@Override
	public void calc(ComContext context) {

		LOG.info(Thread.currentThread().getName(), "open");

		SplitResult[] splitResult = context.getObj("splitResult");

		Iterable <TreeParaContainer> inputSolution = context.getObj("tree");
		for (TreeParaContainer container : inputSolution) {

			//initial tree
			if (container.getTreeParas().size() == 0) {
				container.add(new TreeParaContainer.TreeParas(treeDepth + 1));
				//notice: need to initialize "childHash" here
				TreeParaContainer.TreeParas newTree = container.getTreeParas().get(container.getCursor());
				newTree.setChildHash(new ArrayList <>(newTree.getFeatureIdx().length));
				for (int index = 0; index < newTree.getFeatureIdx().length; index++) {
					newTree.getChildHash().add(new ArrayList <>());
				}
			}
			TreeParaContainer.TreeParas curTree = container.getTreeParas().get(container.getCursor());
			int lastDepth = curTree.getDepth();

			//new tree
			//in this situation, still has to update old tree
			//note: no need for new tree if this is last iteration
			if (lastDepth >= treeDepth && container.getTreeParas().size() < treeNum) {
				container.add(new TreeParaContainer.TreeParas(treeDepth + 1));
				//note, need to initialize "childHash" here
				TreeParaContainer.TreeParas newTree = container.getTreeParas().get(container.getCursor());
				newTree.setChildHash(new ArrayList <>(newTree.getFeatureIdx().length));
				for (int index = 0; index < newTree.getFeatureIdx().length; index++) {
					newTree.getChildHash().add(new ArrayList <>());
				}
				//set tree id
				newTree.setTreeId(container.getTreeParas().size() - 1);
			}

			//update split feature&point
			for (int nodeId = TreeParaContainer.TreeParas.depthStart(lastDepth);
				 nodeId < TreeParaContainer.TreeParas.depthStart(lastDepth + 1); nodeId++) {
				curTree.getFeatureIdx()[nodeId] =
					splitResult[nodeId - TreeParaContainer.TreeParas.depthStart(lastDepth)].getSplitFeature();
				curTree.getSplitPoints()[nodeId] =
					splitResult[nodeId - TreeParaContainer.TreeParas.depthStart(lastDepth)].getSplitPoint();
			}

			//update hash for categorical feature
			for (int nodeId = TreeParaContainer.TreeParas.depthStart(lastDepth);
				 nodeId < TreeParaContainer.TreeParas.depthStart(lastDepth + 1); nodeId++) {
				boolean[] curHash = splitResult[nodeId - TreeParaContainer.TreeParas.depthStart(lastDepth)].getLeftSubset().clone();
				if (curHash.length == 0) {
					//not categorical
					continue;
				}

				//notice: gbdt model needs "right subset" hash table
				//so need !curHash
				ArrayList <Boolean> curRealHash = new ArrayList <>(curHash.length);
				for (boolean hash : curHash) {
					curRealHash.add(!hash);
				}

				curTree.getIsCategorical()[nodeId] = true;
				curTree.getChildHash().set(nodeId, curRealHash);
			}

			//update leaf gain
			//the predict logic use counter for every node, not just leaf node
			for (int nodeId = TreeParaContainer.TreeParas.depthStart(lastDepth);
				 nodeId < TreeParaContainer.TreeParas.depthStart(lastDepth + 1); nodeId++) {
				curTree.getCounters()[TreeParaContainer.TreeParas.childLeftIdx(nodeId)] =
					splitResult[nodeId - TreeParaContainer.TreeParas.depthStart(lastDepth)].getLeftCounter();
				curTree.getCounters()[TreeParaContainer.TreeParas.childRightIdx(nodeId)] =
					splitResult[nodeId - TreeParaContainer.TreeParas.depthStart(lastDepth)].getRightCounter();
			}

			//update depth
			curTree.incDepth();

			break;
		}

		LOG.info(Thread.currentThread().getName(), "close");
	}
}
