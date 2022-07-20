package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.PoissonSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.HashWithReplacementParams;
import com.alibaba.alink.params.dataproc.StratifiedSampleParams;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * StratifiedSample with given ratio with/without replacement.
 */

@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@ParamSelectColumnSpec(name = "strataCol", portIndices = 0)
@NameCn("分层随机采样")
public final class StratifiedSampleBatchOp extends BatchOperator <StratifiedSampleBatchOp>
	implements StratifiedSampleParams <StratifiedSampleBatchOp>, HashWithReplacementParams <StratifiedSampleBatchOp> {

	private static final long serialVersionUID = 8815784097940967758L;

	public StratifiedSampleBatchOp() {
		this(new Params());
	}

	public StratifiedSampleBatchOp(Params params) {
		super(params);
	}

	@Override
	public StratifiedSampleBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		// compute index of group key
		int index = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), getStrataCol());

		DataSet <Row> res = in
			.getDataSet()
			.groupBy(index)
			.reduceGroup(
				new StratifiedSampleReduce(getWithReplacement(), 2020, index, getStrataRatio(), getStrataRatios()));
		this.setOutput(res, in.getSchema());
		return this;
	}

	private class StratifiedSampleReduce<T> extends RichGroupReduceFunction <T, T> {
		private static final long serialVersionUID = 2257608997204962490L;
		private boolean withReplacement;
		private long seed;
		private int index;
		private Double sampleRatio;
		private Map <Object, Double> fractionMap;

		public StratifiedSampleReduce(boolean withReplacement, long seed, int index, Double ratio, String ratios) {
			this.withReplacement = withReplacement;
			this.seed = seed;
			this.index = index;
			this.sampleRatio = ratio;
			fractionMap = new HashMap <>();
			String[] keyRatios = ratios.split(",");
			for (String keyRatio : keyRatios) {
				String[] ratioArray = keyRatio.split(":");
				Double groupRatio = new Double(ratioArray[1]);
				AkPreconditions.checkArgument(groupRatio >= 0.0 && groupRatio <= 1.0,
					new AkIllegalArgumentException("Ratio must be in range [0, 1]."));
				fractionMap.put(ratioArray[0], groupRatio);
			}

		}

		@Override
		public void reduce(Iterable <T> values, Collector <T> out) {
			GetFirstIterator iterator = new GetFirstIterator(values.iterator());
			Double fraction = sampleRatio;
			if (null == fraction || fraction < 0) {
				Row first = (Row) iterator.getFirst();
				if (null != first) {
					Object key = first.getField(index);
					fraction = fractionMap.get(String.valueOf(key));
					AkPreconditions.checkNotNull(fraction, key + " is not contained in map!");
				} else {
					return;
				}
			}

			RandomSampler <T> sampler;
			long seedAndIndex = this.seed + (long) this.getRuntimeContext().getIndexOfThisSubtask();
			if (withReplacement) {
				sampler = new PoissonSampler <T>(fraction, seedAndIndex);
			} else {
				sampler = new BernoulliSampler <T>(fraction, seedAndIndex);
			}
			Iterator <T> sampled = sampler.sample(iterator);
			while (sampled.hasNext()) {
				out.collect(sampled.next());
			}
		}
	}

	static class GetFirstIterator<E> implements Iterator <E> {
		private Iterator <E> originIterator;
		private E first;

		public GetFirstIterator(Iterator <E> iterator) {
			this.originIterator = iterator;
			if (this.originIterator.hasNext()) {
				first = this.originIterator.next();
			}
		}

		public E getFirst() {
			return first;
		}

		@Override
		public boolean hasNext() {
			return (null != first || originIterator.hasNext());
		}

		@Override
		public E next() {
			if (null != first) {
				E tmp = first;
				first = null;
				return tmp;
			} else {
				return originIterator.next();
			}
		}
	}

}

