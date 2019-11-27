package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class ConstructLocalBin extends ComputeFunction {

	private static final Logger LOG = LoggerFactory.getLogger(ConstructLocalBin.class);
	private int binNum;
	private int featureColNum;
	private int maxLevelNumNode;
	private int algoType;
	private float[] predValue;
	private Integer[] predIndex;
	private float[] predLabelForInverse;
	private Integer[] predIndexForInverse;
	private int[] predRank;
	private float[] gradientArray;
	private float[] hessianArray;
	private float[] dcgDiscount;
	private float[] allGradient;
	private float[] allHessian;
	private double[] localRegressionLabelCounters;

	public ConstructLocalBin(int algoType, int binNum, int featureColNum, int treeDepth) {
		this.algoType = algoType;
		this.binNum = binNum;
		this.featureColNum = featureColNum;
		this.maxLevelNumNode = TreeParaContainer.TreeParas.depthSize(treeDepth);
		predValue = new float[0];
		predIndex = new Integer[0];
		predLabelForInverse = new float[0];
		predIndexForInverse = new Integer[0];
		predRank = new int[0];
		gradientArray = new float[0];
		hessianArray = new float[0];
		allGradient = new float[0];
		allHessian = new float[0];

		int kMaxPosition = 10000;
		dcgDiscount = new float[kMaxPosition];
		for (int i = 0; i < kMaxPosition; ++i) {
			dcgDiscount[i] = ((Double) (Math.log(2.0) / Math.log(2.0 + i))).floatValue();
		}
	}

	@Override
	public void calc(ComContext context) {
		LOG.info("{}: {}", this.getClass().getSimpleName(), "start");

		double[] regressionLabelCounters = context.getObj("gbdtBin");
		if (regressionLabelCounters == null) {
			regressionLabelCounters = new double[maxLevelNumNode * featureColNum * binNum * 4];
			context.putObj("gbdtBin", regressionLabelCounters);
		}

		//init counter
		int numCounter = 0;
		for (int k = 0; k < maxLevelNumNode; k++) {
			for (int j = 0; j < featureColNum; j++) {
				for (int i = 0; i < binNum; i++) {
					regressionLabelCounters[numCounter * 4] = 0;
					regressionLabelCounters[numCounter * 4 + 1] = 0;
					regressionLabelCounters[numCounter * 4 + 2] = 0;
					regressionLabelCounters[numCounter * 4 + 3] = 0;
					numCounter++;
				}
			}
		}

		int curOverFlag = 0;
		int gradientCaled = 0;

		byte[] curHashArray = context.getObj("trainDetailArray");
		ByteBuffer buf = ByteBuffer.wrap(curHashArray);

		//node_id and yhat
		byte[] nodeIdBuf = context.getObj("nodeIdBuf");
		float[] predBuf = context.getObj("predBuf");

		byte[] featureCutArray = context.getObj("featureCutArray");

		//features
		byte[] inputFeatures = context.getObj("featuresArray");

		//update dataNum
		int dataNum = inputFeatures.length / featureColNum;

		//combine counter by feature
		int featureIndexSt = 0;
		int featureIndexEd = featureColNum;
		int featureIndexAdd = 1;
		if (context.getStepNo() % 2 == 0) {
			featureIndexSt = featureColNum - 1;
			featureIndexEd = -1;
			featureIndexAdd = -1;
		}

		if (algoType == 0) {
			int validInstanceSize = 0;
			if (predRank.length == 0) {
				predRank = new int[dataNum];
				gradientArray = new float[dataNum];
				allGradient = new float[dataNum];
				localRegressionLabelCounters = new double[maxLevelNumNode * binNum * 4];
			}

			int maxNodeId = 0;

			for (int i = 0; i < dataNum; ++i) {
				int nodeId = nodeIdBuf[i];
				if (nodeId < 0) {
					continue;
				}

				if (nodeId > maxNodeId) {
					maxNodeId = nodeId;
				}

				float gradient = predBuf[i] - buf.getFloat(i * 4);
				gradientArray[validInstanceSize] = gradient;
				allGradient[validInstanceSize] = gradient * gradient;
				predRank[validInstanceSize] = i;
				validInstanceSize++;
			}

			maxNodeId++;

			for (int featureIndex = featureIndexSt;
				 featureIndexAdd * featureIndex < featureIndexEd * featureIndexAdd;
				 featureIndex += featureIndexAdd) {
				byte curFeatureCut = featureCutArray[featureIndex];
				if (curFeatureCut == 2) {
					//if feature sub sampled, ignore this feature
					continue;
				}

				int curIndexForFeature = featureIndex * dataNum;
				Arrays.fill(localRegressionLabelCounters, 0.0);
				for (int i = 0; i < validInstanceSize; ++i) {
					int nodeId = nodeIdBuf[predRank[i]];
					int curValue = inputFeatures[curIndexForFeature + predRank[i]];
					if (curValue < 0) {
						curValue += 256;
					}
					int counterIndex = (nodeId * binNum + curValue) * 4;
					localRegressionLabelCounters[counterIndex] += allGradient[i];
					localRegressionLabelCounters[counterIndex + 1] += gradientArray[i];
					localRegressionLabelCounters[counterIndex + 2] += 1.;
					localRegressionLabelCounters[counterIndex + 3] += 1.;
				}

				for (int i = 0; i < maxNodeId; ++i) {
					System.arraycopy(
						localRegressionLabelCounters, i * binNum * 4,
						regressionLabelCounters, (i * featureColNum * binNum + featureIndex * binNum) * 4,
						binNum * 4
					);
				}
			}

			LOG.info("{}: {}", this.getClass().getSimpleName(), "close least-square loss");
			return;
		}


		if (algoType == 1) {
			int validInstanceSize = 0;
			if (predRank.length == 0) {
				predRank = new int[dataNum];
				gradientArray = new float[dataNum];
				allGradient = new float[dataNum];
				hessianArray = new float[dataNum];
				localRegressionLabelCounters = new double[maxLevelNumNode * binNum * 4];
			}

			int maxNodeId = 0;

			for (int i = 0; i < dataNum; ++i) {
				int nodeId = nodeIdBuf[i];
				if (nodeId < 0) {
					continue;
				}

				if (nodeId > maxNodeId) {
					maxNodeId = nodeId;
				}

				float curPred = predBuf[i];
				float curLabel = buf.getFloat(i * 4);
				double prob = 1.0 / (1.0 + Math.exp(-curPred));
				float gradient = (float) (prob - curLabel);
				float hessian = (float) (prob * (1.0 - prob));
				gradientArray[validInstanceSize] = gradient;
				allGradient[validInstanceSize] = gradient * gradient;
				hessianArray[validInstanceSize] = hessian;
				predRank[validInstanceSize] = i;
				validInstanceSize++;
			}

			maxNodeId++;

			for (int featureIndex = featureIndexSt;
				 featureIndexAdd * featureIndex < featureIndexEd * featureIndexAdd;
				 featureIndex += featureIndexAdd) {
				byte curFeatureCut = featureCutArray[featureIndex];
				if (curFeatureCut == 2) {
					//if feature sub sampled, ignore this feature
					continue;
				}

				int curIndexForFeature = featureIndex * dataNum;
				Arrays.fill(localRegressionLabelCounters, 0.0);
				for (int i = 0; i < validInstanceSize; ++i) {
					int nodeId = nodeIdBuf[predRank[i]];
					int curValue = inputFeatures[curIndexForFeature + predRank[i]];
					if (curValue < 0) {
						curValue += 256;
					}
					int counterIndex = (nodeId * binNum + curValue) * 4;
					localRegressionLabelCounters[counterIndex] += allGradient[i];
					localRegressionLabelCounters[counterIndex + 1] += gradientArray[i];
					localRegressionLabelCounters[counterIndex + 2] += hessianArray[i];
					localRegressionLabelCounters[counterIndex + 3] += 1.;
				}

				for (int i = 0; i < maxNodeId; ++i) {
					System.arraycopy(
						localRegressionLabelCounters, i * binNum * 4,
						regressionLabelCounters, (i * featureColNum * binNum + featureIndex * binNum) * 4,
						binNum * 4
					);
				}
			}

			LOG.info("{}: {}", this.getClass().getSimpleName(), "close log-likelihood loss");
			return;
		}

		for (int featureIndex = featureIndexSt;
			 featureIndexAdd * featureIndex < featureIndexEd * featureIndexAdd;
			 featureIndex += featureIndexAdd) {

			//check feature_index
			//notice the "feature cut" is stored at the back of array
			byte curFeatureCut = featureCutArray[featureIndex];

			if (curFeatureCut == 2) {
				//if feature sub sampled, ignore this feature
				continue;
			}
			int curIndexForFeature = featureIndex * dataNum;

			int indexSt = 0;
			int indexAdd = 1;
			for (int curIndex = indexSt; indexAdd * curIndex < indexAdd * dataNum; curIndex += indexAdd) {

				//curOverFlag=1 : need to construct a new tree, ignore node_id
				//notice when this happens, there're possibly data already processed(with a smaller node_id)
				//so need to deal with them later
				int nodeId = nodeIdBuf[curIndex];

				//nodeID<0 : not sampled
				if (nodeId < 0) {
					continue;
				}

				if (nodeId >= maxLevelNumNode) {
					curOverFlag = 1;
				}

				//get gradient and hessian
				double curPred = predBuf[curIndex];
				double curLabel;
				if (algoType == 0 || algoType == 1) {
					curLabel = buf.getFloat(curIndex * 4);
				} else {
					curLabel = buf.getFloat(curIndex * 8);
				}
				double curGradient = 0;
				double curHessian = 0;
				if (algoType == 0) {
					//regression
					curGradient = curPred - curLabel;
					curHessian = 1.0;
				} else if (algoType == 1) {
					double curProb = 1.0 / (1.0 + Math.exp(-curPred));
					curGradient = (curProb - curLabel);
					curHessian = curProb * (1.0 - curProb);
				} else if (algoType == 2 || algoType == 3 || algoType == 4) {
					//get range & data
					int curEndIndex = buf.getInt(curIndex * 8 + 4);
					int dataCount = curEndIndex - curIndex;

					//get gradient/hessian
					if (gradientCaled == 0) {

						if (predIndex.length < dataCount) {
							predValue = new float[dataCount];
							predIndex = new Integer[dataCount];
							predLabelForInverse = new float[dataCount];
							predIndexForInverse = new Integer[dataCount];
							predRank = new int[dataCount];
							gradientArray = new float[dataCount];
							hessianArray = new float[dataCount];
						}
						for (int detIndex = 0; detIndex < dataCount; detIndex++) {
							predValue[detIndex] = predBuf[(detIndex + curIndex)];
							predIndex[detIndex] = detIndex;
							predLabelForInverse[detIndex] = buf.getFloat((detIndex + curIndex) * 8);
							predIndexForInverse[detIndex] = detIndex;
							gradientArray[detIndex] = 0;
							hessianArray[detIndex] = 0;
						}

						//sort data by label(to get inverse max DCG)
						Arrays.sort(
							predIndexForInverse, 0, dataCount,
							new FloatIndexGtComparator(predLabelForInverse)
						);

						//inverse max DCG
						double max_dcg = 0;
						for (int invIndex = 0; invIndex < dataCount; invIndex++) {
							max_dcg += dcgDiscount[invIndex] * predLabelForInverse[predIndexForInverse[invIndex]];
						}
						double inverseMaxDCG = 1.0 / max_dcg;

						//sort data by current prediction
						Arrays.sort(predIndex, 0, dataCount, new FloatIndexGtComparator(predValue));
						for (int detIndex = 0; detIndex < dataCount; detIndex++) {
							predRank[predIndex[detIndex]] = detIndex;
						}

						double bestScore = predValue[predIndex[0]];
						double worstScore = predValue[predIndex[dataCount - 1]];

						//cal gradient & hessian
						for (int index1 = 0; index1 < dataCount; index1++) {
							double highLabel = buf.getFloat((index1 + curIndex) * 8);
							float highPred = predValue[index1];
							int highRank = predRank[index1];
							for (int index2 = 0; index2 < dataCount; index2++) {
								if (index2 == index1) {
									continue;
								}
								float lowPred = predValue[index2];
								double lowLabel = buf.getFloat((index2 + curIndex) * 8);
								int lowRank = predRank[index2];

								if (algoType == 2 || algoType == 3) {
									//cal once for each pair
									if (lowLabel >= highLabel) {
										continue;
									}

									// score diff
									double deltaScore = highPred - lowPred;
									// get dcg gap
									double dcgGap = highLabel - lowLabel;
									// get discount of this pair
									double pairedDiscount = Math.abs(dcgDiscount[highRank] - dcgDiscount[lowRank]);
									// get delta NDCG
									double deltaPairNDCG = dcgGap * pairedDiscount;
									if (highLabel != lowLabel && bestScore != worstScore) {
										deltaPairNDCG /= (0.01f + Math.abs(deltaScore));
									}
									// inverse dcg
									if (algoType == 2) {
										deltaPairNDCG *= inverseMaxDCG;
									}
									// calculate lambda for this pair
									double pLambda = 2.0f / (1.0f + Math.exp(2.0 * deltaScore));
									double pHessian = pLambda * (2.0f - pLambda);

									// update
									pLambda *= -deltaPairNDCG;
									pHessian *= 2 * deltaPairNDCG;

									gradientArray[index1] += pLambda;
									gradientArray[index2] -= pLambda;
									hessianArray[index1] += pHessian;
									hessianArray[index2] += pHessian;
								} else if (algoType == 4) {
									//cal once for each pair
									if (lowLabel >= highLabel) {
										continue;
									}

									double tau = 0.6;

									// score diff
									double deltaScore = highPred - lowPred;
									if (deltaScore >= tau) {
										continue;
									}
									gradientArray[index1] += -(lowPred + tau);
									gradientArray[index2] += -(highPred - tau);
									hessianArray[index1] += 1;
									hessianArray[index2] += 1;
								} else {
									assert (false);
								}
							}
						}
						//copy to big array
						if (allGradient.length < dataNum) {
							allGradient = new float[dataNum];
							allHessian = new float[dataNum];
						}
						System.arraycopy(gradientArray, 0, allGradient, curIndex, dataCount);
						System.arraycopy(hessianArray, 0, allHessian, curIndex, dataCount);
					} else {
						System.arraycopy(allGradient, curIndex, gradientArray, 0, dataCount);
						System.arraycopy(allHessian, curIndex, hessianArray, 0, dataCount);
					}

					//set gradient and hessian
					for (int detIndex = 0; detIndex < dataCount; detIndex++) {

						if ((gradientArray[detIndex] < 1e-7 && gradientArray[detIndex] > -1e-7) ||
							(hessianArray[detIndex] < 1e-7 && hessianArray[detIndex] > -1e-7)) {
							gradientArray[detIndex] = 0;
							hessianArray[detIndex] = 0;
						}
						int nodeIdDet = nodeIdBuf[(curIndex + detIndex)];
						if (nodeIdDet >= maxLevelNumNode) {
							curOverFlag = 1;
						}
						int nodeHead = nodeIdDet * featureColNum * binNum + featureIndex * binNum;
						if (curOverFlag == 1) {
							nodeHead = featureIndex * binNum;
						}

						//combine
						int curFeature = inputFeatures[curIndexForFeature + curIndex + detIndex];

						//deal with features in range[128,256)
						if (curFeature < 0) {
							curFeature += 256;
						}
						int counterIndex = (nodeHead + curFeature) * 4;

						//order: square_sum, sum, weight_sum, number_sum
						regressionLabelCounters[counterIndex] += gradientArray[detIndex] * gradientArray[detIndex];
						regressionLabelCounters[counterIndex + 1] += gradientArray[detIndex];
						regressionLabelCounters[counterIndex + 2] += hessianArray[detIndex];
						regressionLabelCounters[counterIndex + 3] += 1.;
					}

					//move to next group (rather than next data)
					curIndex += dataCount - 1;

					continue;
				} else {
					assert (false);
				}

				int node_head = nodeId * featureColNum * binNum + featureIndex * binNum;
				if (curOverFlag == 1) {
					node_head = featureIndex * binNum;
				}

				//combine
				int curFeature = inputFeatures[curIndexForFeature + curIndex];

				//deal with features in range[128,256)
				if (curFeature < 0) {
					curFeature += 256;
				}
				int counterIndex = (node_head + curFeature) * 4;
				//order: square_sum, sum, weight_sum, number_sum
				regressionLabelCounters[counterIndex] += curGradient * curGradient;
				regressionLabelCounters[counterIndex + 1] += curGradient;
				regressionLabelCounters[counterIndex + 2] += curHessian;
				regressionLabelCounters[counterIndex + 3] += 1.;
			}

			gradientCaled = 1;
		}

		//this is where we process the data wrongly processed
		//ignore node_id when curOverFlag == 1
		if (curOverFlag == 1) {
			for (int tree_index = 1; tree_index < maxLevelNumNode; tree_index++) {
				for (int i = 0; i < featureColNum; ++i) {
					for (int j = 0; j < binNum; ++j) {
						int newIndex = (i * binNum + j) * 4;
						int oldIndex = (tree_index * featureColNum * binNum + i * binNum + j) * 4;
						regressionLabelCounters[newIndex] += regressionLabelCounters[oldIndex];
						regressionLabelCounters[newIndex + 1] += regressionLabelCounters[oldIndex + 1];
						regressionLabelCounters[newIndex + 2] += regressionLabelCounters[oldIndex + 2];
						regressionLabelCounters[newIndex + 3] += regressionLabelCounters[oldIndex + 3];
						regressionLabelCounters[oldIndex] = 0;
						regressionLabelCounters[oldIndex + 1] = 0;
						regressionLabelCounters[oldIndex + 2] = 0;
						regressionLabelCounters[oldIndex + 3] = 0;
					}
				}
			}
		}

		LOG.info("{}: {}", this.getClass().getSimpleName(), "close");
	}

	public static class FloatIndexGtComparator
		implements Comparator<Integer> {

		private float[] data;

		public FloatIndexGtComparator(float[] data) {
			this.data = data;
		}

		@Override
		public int compare(Integer o1, Integer o2) {
			if (data[o1] < data[o2]) {
				return 1;
			}
			if (data[o1] > data[o2]) {
				return -1;
			}
			return 0;
		}
	}

}