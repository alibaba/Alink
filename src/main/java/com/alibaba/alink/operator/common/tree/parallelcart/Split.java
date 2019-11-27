package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Random;

public class Split extends ComputeFunction {
	private static final Logger LOG = LoggerFactory.getLogger(Split.class);
	private int featureColNum;
	private int maxNodeNum;
	private int dataNum;
	private int algoType;
	private double subSamplingRatio;
	private double featureSubSamplingRatio;
	private HashMap<Long, SplitResult> curHashResult;
	private int iterCount;
	private Random random;

	public Split(int algoType, int featureColNum, int treeDepth,
				 double subSamplingRatio, double featureSubSamplingRatio, int seed) {
		this.algoType = algoType;
		this.featureColNum = featureColNum;
		this.maxNodeNum = TreeParaContainer.TreeParas.depthSize(treeDepth);
		this.subSamplingRatio = subSamplingRatio;
		this.featureSubSamplingRatio = featureSubSamplingRatio;

		random = new Random(seed);
		iterCount = 0;
	}

	public boolean isLeft(SplitResult curSplitResult, byte[] input_features, int curIndex) {

		//no split, all data to left child by default
		if (curSplitResult.getSplitFeature() < 0) {
			return true;
		}

		boolean isLeft = false;
		int curFeature = input_features[curIndex + curSplitResult.getSplitFeature() * dataNum];
		//deal with feature in range [128,256)
		if (curFeature < 0) {
			curFeature += 256;
		}
		if (curSplitResult.getLeftSubset().length > 0) {
			//categorical
			if (curSplitResult.getLeftSubset()[curFeature]) {
				isLeft = true;
			}
		} else {
			//continuous
			if (curFeature <= curSplitResult.getSplitPoint()) {
				isLeft = true;
			}
		}
		return isLeft;
	}

	@Override
	public void calc(ComContext context) {

		LOG.info(Thread.currentThread().getName(), "open");

		//only 1 variable
		curHashResult = new HashMap<>();

		SplitResult[] curResult = context.getObj("splitResult");
		//trans list to hash map
		for (int spIndex = 0; spIndex < curResult.length; spIndex++) {
			curHashResult.put((long) spIndex, curResult[spIndex]);
		}

		//hash array
		byte[] curHashArray = context.getObj("trainDetailArray");
		ByteBuffer buf = ByteBuffer.wrap(curHashArray);

		byte[] nodeIdBuf = context.getObj("nodeIdBuf");
		float[] predBuf = context.getObj("predBuf");

		byte[] featureCutArray = context.getObj("featureCutArray");

		byte[] inputFeatures = context.getObj("featuresArray");

		//update dataNum
		dataNum = inputFeatures.length / featureColNum;

		//perform split
		SplitResult splitResultZero = curHashResult.get((long) 0);
		int treeId = splitResultZero.getMaxTreeId();
		if (treeId >= maxNodeNum - 1) {
			iterCount++;
		}

		int curGroupEnd = -1;
		for (int curIndex = 0; curIndex < dataNum; curIndex++) {
			//update group end
			int isNewGroup = 0;
			if (algoType >= 2) {
				if (curGroupEnd == -1 || curIndex >= curGroupEnd) {
					isNewGroup = 1;
					curGroupEnd = buf.getInt(curIndex * 8 + 4);
				}
			}
			int lastNodeId = nodeIdBuf[curIndex];

			int realLastNodeId = lastNodeId;

			//minus value means not sampled during tree building
			//this group of data doesn't contribute when building the tree
			//but still needs to update "node" and score
			if (realLastNodeId < 0) {
				realLastNodeId = -lastNodeId - 1;
			}

			if (treeId >= maxNodeNum - 1) {
				//notice: only need to maintain gradient for regression
				//no need to maintain pred & hessian
				double curPred = predBuf[curIndex];

				SplitResult realSplitResult = curHashResult.get((long) realLastNodeId);
				if (isLeft(realSplitResult, inputFeatures, curIndex)) {
					double average = realSplitResult.getLeftCounter().getSum()
						/ realSplitResult.getLeftCounter().getWeightSum();
					//special process for GBRank loss
					if (algoType == 4) {
						predBuf[curIndex] = (float) (((iterCount - 1) * curPred + average) / iterCount);
					} else {
						predBuf[curIndex] = (float) (curPred + average);
					}
				} else {
					double average = realSplitResult.getRightCounter().getSum()
						/ realSplitResult.getRightCounter().getWeightSum();

					if (algoType == 4) {
						predBuf[curIndex] = (float) (((iterCount - 1) * curPred + average) / iterCount);
					} else {
						predBuf[curIndex] = (float) (curPred + average);
					}
				}

				//now build next tree
				//sub sampling here
				//for learning to rank, needs to sub sampling groups(rather than data)
				if (algoType >= 2) {
					if (isNewGroup == 0) {
						nodeIdBuf[curIndex] = nodeIdBuf[curIndex - 1];
					} else {
						if (random.nextDouble() < subSamplingRatio) {
							nodeIdBuf[curIndex] = 0;
						} else {
							nodeIdBuf[curIndex] = -1;
						}
					}
				} else {
					if (random.nextDouble() < subSamplingRatio) {
						nodeIdBuf[curIndex] = 0;
					} else {
						nodeIdBuf[curIndex] = -1;
					}
				}

				continue;
			}

			SplitResult curSplitResult = curHashResult.get((long) realLastNodeId);

			//put node_id
			//notice: if this data is sub sampled, its node_id is (-real_node_id-1)
			//so its left child is node_id*2+1, right child is node_id*2
			//(exactly opposite to normal data)
			if (isLeft(curSplitResult, inputFeatures, curIndex)) {
				//left
				if (lastNodeId < 0) {
					//sub sampled nodes
					nodeIdBuf[curIndex] = ((Integer) (2 * lastNodeId + 1)).byteValue();
				} else {
					//normal nodes
					nodeIdBuf[curIndex] = ((Integer) (2 * lastNodeId)).byteValue();
				}
			} else {
				//right
				if (lastNodeId < 0) {
					//sub sampled nodes
					nodeIdBuf[curIndex] = ((Integer) (2 * lastNodeId)).byteValue();
				} else {
					//normal nodes
					nodeIdBuf[curIndex] = ((Integer) (2 * lastNodeId + 1)).byteValue();
				}
			}
		}

		//new, set feature cut
		if (treeId < maxNodeNum - 1) {
			//old tree
			//no need to operate
		} else {
			//new tree
			//feature sub sampling
			for (int index = 0; index < featureColNum; index++) {
				if (random.nextDouble() < featureSubSamplingRatio) {
					featureCutArray[index] = 0;
				} else {
					//set to 2 (means feature sub sampling)
					featureCutArray[index] = 2;
				}
			}
		}

		LOG.info(Thread.currentThread().getName(), "close");
	}

}
