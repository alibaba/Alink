package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.params.statistics.QuantileBatchParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * In statistics and probability quantiles are cut points dividing
 * the range of a probability distribution into contiguous intervals
 * with equal probabilities, or dividing the observations in a sample
 * in the same way.
 * (https://en.wikipedia.org/wiki/Quantile)
 * <p>
 * reference: Yang, X. (2014). Chong gou da shu ju tong ji (1st ed., pp. 25-29).
 * <p>
 * Note: This algorithm is improved on the base of the parallel
 * sorting by regular sampling(PSRS). The following step is added
 * to the PSRS
 * <ul>
 * <li>replace (val) with (val, task id) to distinguishing the
 * same value on different machines</li>
 * <li>
 * the index of q-quantiles: index = roundMode((n - 1) * k / q),
 * n is the count of sample, k satisfying 0 < k < q
 * </li>
 * </ul>
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("分位数")
@NameEn("Quantile")
public final class QuantileBatchOp extends BatchOperator <QuantileBatchOp>
	implements QuantileBatchParams <QuantileBatchOp> {

	private static final long serialVersionUID = -86119177892147044L;

	public QuantileBatchOp() {
		super(null);
	}

	public QuantileBatchOp(Params params) {
		super(params);
	}

	@Override
	public QuantileBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		TableSchema tableSchema = in.getSchema();

		String quantileColName = getSelectedCol();

		int index = TableUtil.findColIndexWithAssertAndHint(tableSchema.getFieldNames(), quantileColName);

		/* filter the selected column from input */
		DataSet <Row> input = in.select(quantileColName).getDataSet();

		/* sort data */
		Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sortedData
			= SortUtils.pSort(input, 0);

		/* calculate quantile */
		DataSet <Row> quantile = sortedData.f0.
			groupBy(0)
			.reduceGroup(new Quantile(
				0, getQuantileNum(),
				getRoundMode()))
			.withBroadcastSet(sortedData.f1, "counts");

		/* set output */
		setOutput(quantile,
			new String[] {tableSchema.getFieldNames()[index], "quantile"},
			new TypeInformation <?>[] {tableSchema.getFieldTypes()[index], BasicTypeInfo.LONG_TYPE_INFO});

		return this;
	}

	/**
	 *
	 */
	public static class Quantile extends RichGroupReduceFunction <Tuple2 <Integer, Row>, Row> {
		private static final long serialVersionUID = -6101513604891658021L;
		private int index;
		private List <Tuple2 <Integer, Long>> counts;
		private long countSum = 0;
		private int quantileNum;
		private RoundMode roundType;

		public Quantile(int index, int quantileNum, RoundMode roundType) {
			this.index = index;
			this.quantileNum = quantileNum;
			this.roundType = roundType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.counts = getRuntimeContext().getBroadcastVariableWithInitializer(
				"counts",
				new BroadcastVariableInitializer <Tuple2 <Integer, Long>, List <Tuple2 <Integer, Long>>>() {
					@Override
					public List <Tuple2 <Integer, Long>> initializeBroadcastVariable(
						Iterable <Tuple2 <Integer, Long>> data) {
						// sort the list by task id to calculate the correct offset
						List <Tuple2 <Integer, Long>> sortedData = new ArrayList <>();
						for (Tuple2 <Integer, Long> datum : data) {
							sortedData.add(datum);
						}
						Collections.sort(sortedData, new Comparator <Tuple2 <Integer, Long>>() {
							@Override
							public int compare(Tuple2 <Integer, Long> o1, Tuple2 <Integer, Long> o2) {
								return o1.f0.compareTo(o2.f0);
							}
						});

						return sortedData;
					}
				});

			for (int i = 0; i < this.counts.size(); ++i) {
				countSum += this.counts.get(i).f1;
			}
		}

		@Override
		public void reduce(Iterable <Tuple2 <Integer, Row>> values, Collector <Row> out) throws Exception {
			ArrayList <Row> allRows = new ArrayList <>();
			int id = -1;
			long start = 0;
			long end = 0;

			for (Tuple2 <Integer, Row> value : values) {
				id = value.f0;
				allRows.add(Row.copy(value.f1));
			}

			if (id < 0) {
				throw new Exception("Error key. key: " + id);
			}

			int curListIndex = -1;
			int size = counts.size();

			for (int i = 0; i < size; ++i) {
				int curId = counts.get(i).f0;

				if (curId == id) {
					curListIndex = i;
					break;
				}

				if (curId > id) {
					throw new Exception("Error curId: " + curId
						+ ". id: " + id);
				}

				start += counts.get(i).f1;
			}

			end = start + counts.get(curListIndex).f1;

			if (allRows.size() != end - start) {
				throw new Exception("Error start end."
					+ " start: " + start
					+ ". end: " + end
					+ ". size: " + allRows.size());
			}

			SortUtils.RowComparator rowComparator = new SortUtils.RowComparator(this.index);
			Collections.sort(allRows, rowComparator);

			QuantileDiscretizerTrainBatchOp.QIndex qIndex = new QuantileDiscretizerTrainBatchOp.QIndex(
				countSum, quantileNum, roundType);

			for (int i = 0; i <= quantileNum; ++i) {
				long index = qIndex.genIndex(i);

				if (index >= start && index < end) {
					out.collect(
						RowUtil.merge(allRows.get((int) (index - start)), Long.valueOf(i)));
				}
			}
		}

	}

}
