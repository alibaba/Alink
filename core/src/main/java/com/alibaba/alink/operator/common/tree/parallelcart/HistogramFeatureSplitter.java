package com.alibaba.alink.operator.common.tree.parallelcart;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.Criteria;
import com.alibaba.alink.operator.common.tree.FeatureMeta;
import com.alibaba.alink.operator.common.tree.FeatureSplitter;
import com.alibaba.alink.operator.common.tree.LabelAccessor;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.AlinkCriteria;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.CriteriaType;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.GBMTreeSplitCriteria;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.PaiCriteria;
import com.alibaba.alink.operator.common.tree.parallelcart.criteria.XGboostCriteria;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LossUtils;
import com.alibaba.alink.params.classification.GbdtTrainParams;

import java.util.Arrays;

import static com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp.USE_MISSING;

/**
 * Histogram based feature splitter.
 *
 * <p>Note: It has not supported the missing value, so the missing value in the feature splitter is invalid.
 */
public abstract class HistogramFeatureSplitter extends FeatureSplitter {
	protected double[] featureHist;
	protected Slice slice;

	protected Criteria bestLeft;
	protected Criteria bestRight;

	protected final int maxDepth;
	protected final boolean useInstanceCnt;
	protected final boolean useMissing;

	protected int[] missingSplit;

	public HistogramFeatureSplitter(
		Params params,
		FeatureMeta featureMeta) {
		super(params, featureMeta);
		this.maxDepth = params.get(GbdtTrainParams.MAX_DEPTH);
		this.useInstanceCnt = LossUtils.useInstanceCount(
			params.get(LossUtils.LOSS_TYPE)
		);
		this.useMissing = params.get(USE_MISSING);
	}

	public void reset(double[] featureHist, Slice slice, int depth) {
		this.featureHist = featureHist;
		this.slice = slice;
		this.depth = depth;

		this.canSplit = false;
		this.counted = false;
	}

	@Override
	public FeatureSplitter[][] split(FeatureSplitter[] splitters) {
		throw new UnsupportedOperationException("Unsupported.");
	}

	@Override
	protected void count() {
		if (counted) {
			return;
		}

		total = criteriaOf();
		missing = criteriaOf();

		LabelAccessor totalAccessor = labelAccessorOf(total);

		for (int i = 0; i < totalAccessor.size(); ++i) {
			totalAccessor.add(i);
		}

		if (useMissing) {
			LabelAccessor missingAccessor = labelAccessorOf(missing);
			missingAccessor.add(featureMeta.getMissingIndex());
			total.subtract(missing);
		}

		counted = true;
	}

	interface CategoricalLabelSortable {
		void sort4Categorical(Integer[] compareIndices, int start, int end);
	}

	private class PaiCriteriaHistogramAccessor extends LabelAccessor implements CategoricalLabelSortable {
		final static int step = 4;
		PaiCriteria criteria;

		PaiCriteriaHistogramAccessor(PaiCriteria criteria) {
			this.criteria = criteria;
		}

		@Override
		public void sort4Categorical(Integer[] compareIndices, int start, int end) {
			Arrays.sort(compareIndices, start, end, (o1, o2) -> {
				int startIndex1 = o1 * step + slice.start * step;
				int startIndex2 = o2 * step + slice.start * step;
				return Double.compare(
					featureHist[startIndex1],
					featureHist[startIndex2]
				);
			});
		}

		@Override
		public int size() {
			return slice.end - slice.start;
		}

		@Override
		public void add(int index) {
			int startIndex = index * step + slice.start * step;
			criteria.add(
				featureHist[startIndex],
				featureHist[startIndex + 1],
				featureHist[startIndex + 2],
				(int) featureHist[startIndex + 3]
			);
		}

		@Override
		public void sub(int index) {
			int startIndex = index * step + slice.start * step;
			criteria.subtract(
				featureHist[startIndex],
				featureHist[startIndex + 1],
				featureHist[startIndex + 2],
				(int) featureHist[startIndex + 3]
			);
		}
	}

	private class AlinkCriteriaHistogramAccessor extends LabelAccessor implements CategoricalLabelSortable {
		final static int step = 4;
		AlinkCriteria criteria;

		AlinkCriteriaHistogramAccessor(AlinkCriteria criteria) {
			this.criteria = criteria;
		}

		@Override
		public void sort4Categorical(Integer[] compareIndices, int start, int end) {
			Arrays.sort(compareIndices, start, end, (o1, o2) -> {
				int startIndex1 = o1 * step + slice.start * step;
				int startIndex2 = o2 * step + slice.start * step;
				return Double.compare(
					featureHist[startIndex1] / featureHist[startIndex1 + 1],
					featureHist[startIndex2] / featureHist[startIndex2 + 1]
				);
			});
		}

		@Override
		public int size() {
			return slice.end - slice.start;
		}

		@Override
		public void add(int index) {
			int startIndex = index * step + slice.start * step;
			criteria.add(
				featureHist[startIndex],
				featureHist[startIndex + 1],
				featureHist[startIndex + 2],
				(int) featureHist[startIndex + 3]
			);
		}

		@Override
		public void sub(int index) {
			int startIndex = index * step + slice.start * step;
			criteria.subtract(
				featureHist[startIndex],
				featureHist[startIndex + 1],
				featureHist[startIndex + 2],
				(int) featureHist[startIndex + 3]
			);
		}
	}

	private class XGBoostCriteriaHistogramAccessor extends LabelAccessor implements CategoricalLabelSortable {
		final static int step = 4;
		XGboostCriteria criteria;

		XGBoostCriteriaHistogramAccessor(XGboostCriteria criteria) {
			this.criteria = criteria;
		}

		@Override
		public void sort4Categorical(Integer[] compareIndices, int start, int end) {
			Arrays.sort(compareIndices, start, end, (o1, o2) -> {
				int startIndex1 = o1 * step + slice.start * step;
				int startIndex2 = o2 * step + slice.start * step;
				return Double.compare(
					featureHist[startIndex1] / featureHist[startIndex1 + 1],
					featureHist[startIndex2] / featureHist[startIndex2 + 1]
				);
			});
		}

		@Override
		public int size() {
			return slice.end - slice.start;
		}

		@Override
		public void add(int index) {
			int startIndex = index * step + slice.start * step;
			criteria.add(
				featureHist[startIndex],
				featureHist[startIndex + 1],
				featureHist[startIndex + 2],
				(int) featureHist[startIndex + 3]
			);
		}

		@Override
		public void sub(int index) {
			int startIndex = index * step + slice.start * step;
			criteria.subtract(
				featureHist[startIndex],
				featureHist[startIndex + 1],
				featureHist[startIndex + 2],
				(int) featureHist[startIndex + 3]
			);
		}
	}

	GBMTreeSplitCriteria criteriaOf() {
		switch (params.get(CriteriaType.CRITERIA_TYPE)) {
			case PAI:
				return new PaiCriteria(0, 0, 0, 0);
			case ALINK:
				return new AlinkCriteria(0, 0, 0, 0);
			case XGBOOST:
				return new XGboostCriteria(
					params.get(GbdtTrainParams.LAMBDA),
					params.get(GbdtTrainParams.GAMMA),
					0,
					0,
					0,
					0);
			default:
				throw new IllegalStateException("There should be set the gain type");
		}
	}

	LabelAccessor labelAccessorOf(Criteria criteria) {
		if (criteria instanceof PaiCriteria) {
			return new PaiCriteriaHistogramAccessor((PaiCriteria) criteria);
		} else if (criteria instanceof AlinkCriteria) {
			return new AlinkCriteriaHistogramAccessor((AlinkCriteria) criteria);
		} else if (criteria instanceof XGboostCriteria) {
			return new XGBoostCriteriaHistogramAccessor((XGboostCriteria) criteria);
		} else {
			throw new IllegalStateException("The criteria type must be pai, alink or xgboost.");
		}
	}
}
