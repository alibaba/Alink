package com.alibaba.alink.operator.batch.graph.storage;

public class GraphUtils {
	/**
	 * build the alias table for edges in alias[start, end) and prob[start, end)
	 *
	 * @param start
	 * @param end
	 * @param alias
	 * @param prob
	 */
	public static void buildAliasTable(int start, int end, int[] alias, double[] prob) {
		if (start >= end) {
			return;
		}
		int[] block = new int[end - start];
		int smallBlockId = 0;
		int largeBlockId = end - start - 1;
		double[] tmpProb = new double[end - start];
		double sum = 0;
		for (int i = 0; i < tmpProb.length; i++) {
			tmpProb[i] = prob[i + start];
			sum += tmpProb[i];
		}
		for (int i = 0; i < tmpProb.length; i++) {
			tmpProb[i] = tmpProb[i] / sum * (end - start);
			if (tmpProb[i] < 1.0) {
				block[smallBlockId++] = i;
			} else {
				block[largeBlockId--] = i;
			}
		}

		int smallIdx, largeIdx;
		while (largeBlockId != end - start - 1 && smallBlockId != 0) {
			smallIdx = block[--smallBlockId];
			largeIdx = block[++largeBlockId];
			prob[smallIdx + start] = tmpProb[smallIdx];
			alias[smallIdx + start] = largeIdx;
			tmpProb[largeIdx] = tmpProb[largeIdx] - (1 - tmpProb[smallIdx]);
			if (tmpProb[largeIdx] < 1.0) {
				block[smallBlockId++] = largeIdx;
			} else {
				block[largeBlockId--] = largeIdx;
			}
		}

		while (largeBlockId != end - start - 1) {
			largeIdx = block[++largeBlockId];
			prob[largeIdx + start] = 1;
		}

		while (smallBlockId != 0) {
			smallIdx = block[--smallBlockId];
			prob[smallIdx + start] = 1;
		}
	}

	/**
	 * build partial sum for edges partialSum[start, end)
	 *
	 * @param start
	 * @param end
	 * @param partialSum
	 */
	public static void buildPartialSum(int start, int end, double[] partialSum) {
		if (start >= end) {
			return;
		}
		for (int i = start + 1; i < end; i++) {
			partialSum[i] += partialSum[i - 1];
		}
		for (int i = start; i < end; i++) {
			partialSum[i] /= partialSum[end - 1];
		}
	}
}
