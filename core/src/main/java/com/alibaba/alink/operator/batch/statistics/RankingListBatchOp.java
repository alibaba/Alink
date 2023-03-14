package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.statistics.RankingListParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author yangxu
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "objectCol")
@ParamSelectColumnSpec(name = "statCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "addedCols")
@NameCn("排行榜")
@NameEn("Ranking List")
public final class RankingListBatchOp extends BatchOperator <RankingListBatchOp>
	implements RankingListParams <RankingListBatchOp> {

	private static final Double PRECISION = 1e-16;
	private static final long serialVersionUID = 2673682618359897321L;

	public RankingListBatchOp() {
		super(null);
	}

	public RankingListBatchOp(Params params) {
		super(params);
	}

	@Override
	public RankingListBatchOp linkFrom(BatchOperator <?>... inputs) {

		BatchOperator <?> in = checkAndGetFirst(inputs);

		String groupCol = getGroupCol();
		String[] values = getGroupValues();
		String objectCol = getObjectCol();
		String statCol = getStatCol();
		StatType statFunc = getStatType();
		String[] addedCols = getAddedCols();
		String[] addedStatFuncs = getAddedStatTypes();
		Boolean isDescending = getIsDescending();

		int topN = super.getParams().get(TOP_N);

		if (in.getColNames().length == 0) {
			throw new RuntimeException("table col num must be larger than 0.");
		}

		//check param
		if (objectCol == null || objectCol.isEmpty()) {
			throw new RuntimeException("objectCol must be set.");
		}

		if ((statCol == null || statCol.isEmpty()) &&
			statFunc != StatType.count) {
			throw new RuntimeException("if stat col is null, then statFunc must be count.");
		}
		if (statCol == null || statCol.isEmpty()) {
			statCol = in.getColNames()[0];
		}

		if ((addedCols == null && addedStatFuncs != null) ||
			(addedCols != null && addedStatFuncs == null)) {
			throw new RuntimeException("addedCols and addedStatFuncs length must be same.");
		}

		if (addedCols != null && addedStatFuncs != null &&
			addedCols.length != addedStatFuncs.length) {
			throw new RuntimeException("addedCols and addedStatFuncs length must be same.");
		}

		if (groupCol != null && !groupCol.isEmpty()) {
			if (values == null || values.length == 0) {
				throw new RuntimeException("values must be set.");
			}
		}

		//check type
		TableSchema tableSchema = in.getSchema();
		TypeInformation <?> statColType = null;
		if (statCol != null && !statCol.isEmpty()) {
			statColType = tableSchema.getFieldType(statCol).get();
			if (statColType != AlinkTypes.INT && statColType != AlinkTypes.LONG &&
				statColType != AlinkTypes.DOUBLE &&
				statFunc != StatType.count) {
				throw new RuntimeException("only support count when type not double and long.");
			}
		}

		TypeInformation <?>[] addedColTypes = null;
		if (addedCols != null && addedCols.length != 0) {
			addedColTypes = new TypeInformation <?>[addedCols.length];
			int len = addedCols.length;
			for (int i = 0; i < len; ++i) {
				TableUtil.assertSelectedColExist(in.getColNames(), addedCols[i]);
				TypeInformation <?> type = tableSchema.getFieldType(addedCols[i]).get();
				if (type != AlinkTypes.INT && type != AlinkTypes.LONG && type != AlinkTypes.DOUBLE &&
					!addedStatFuncs[i].equals("count")) {
					throw new RuntimeException("only support count when type not double and long.");
				}
				addedColTypes[i] = type;
			}
		}

		TypeInformation <?> groupColType = null;
		if (groupCol != null && !groupCol.isEmpty()) {
			groupColType = tableSchema.getFieldType(groupCol).get();
			if (groupColType != AlinkTypes.STRING) {
				throw new RuntimeException("group col must be string.");
			}
		}

		if (objectCol == null || objectCol.isEmpty()) {
			throw new RuntimeException("object col must exist.");
		}

		TypeInformation <?> objColType = null;
		if (objectCol != null && !objectCol.isEmpty()) {
			objColType = tableSchema.getFieldType(objectCol).get();
			if (objColType != AlinkTypes.STRING && objColType != AlinkTypes.LONG && objColType != AlinkTypes.INT) {
				throw new RuntimeException("objectCol  must be string or bigint.");
			}
		}

		int groupColIndex = -1;
		if (groupCol != null) {
			StringBuilder tmp = new StringBuilder(groupCol);
			tmp.append("=").append("'").append(values[0]).append("'");
			for (int i = 1; i < values.length; ++i) {
				tmp.append(" or ").append(groupCol).append("=").
					append("'").append(values[i]).append("'");
			}
			String filter = tmp.toString();
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("filter: " + filter);
			}
			in = in.filter(filter);
			groupColIndex = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), groupCol);
		}

		int addedLen = addedCols == null ? 0 : addedCols.length;
		int[] colIdx = new int[addedLen + 2];
		colIdx[0] = TableUtil.findColIndex(in.getColNames(), objectCol);
		colIdx[1] = TableUtil.findColIndex(in.getColNames(), statCol);
		for (int i = 0; i < addedLen; ++i) {
			colIdx[i + 2] = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), addedCols[i]);
		}
		StatType[] funcs = {statFunc};

		if (addedLen > 0) {
			funcs = new StatType[1 + addedLen];
			funcs[0] = statFunc;
			for (int i = 0; i < addedStatFuncs.length; i++) {
				funcs[1 + i] = ParamUtil.searchEnum(RankingListParams.STAT_TYPE, addedStatFuncs[i]);
			}
		}

		TypeInformation <?>[] statTypes = new TypeInformation[addedLen];
		for (int i = 0; i < addedLen; ++i) {
			statTypes[i] = AlinkTypes.DOUBLE;
		}
		if (groupCol == null) {
			String[] outputColNames = {objectCol, statCol};
			TypeInformation <?>[] outputColTypes = {objColType, AlinkTypes.DOUBLE};
			if (addedCols != null && addedLen > 0) {
				outputColNames = ArrayUtils.addAll(outputColNames, addedCols);
				outputColTypes = ArrayUtils.addAll(outputColTypes, statTypes);
			}
			outputColNames = ArrayUtils.add(outputColNames, "rank");
			outputColTypes = ArrayUtils.add(outputColTypes, AlinkTypes.LONG);
			DataSet <Row> sorted = in.getDataSet().reduceGroup(new SortByStatCol(
				colIdx, groupColIndex, funcs, in.getColTypes(),
				isDescending, topN));
			setOutput(sorted, outputColNames, outputColTypes);
		} else {
			String[] outputColNames = {groupCol, objectCol, statCol};
			TypeInformation <?>[] outputColTypes = {groupColType, objColType, AlinkTypes.DOUBLE};
			if (addedCols != null && addedLen > 0) {
				outputColNames = ArrayUtils.addAll(outputColNames, addedCols);
				outputColTypes = ArrayUtils.addAll(outputColTypes, statTypes);
			}
			outputColNames = ArrayUtils.add(outputColNames, "rank");
			outputColTypes = ArrayUtils.add(outputColTypes, AlinkTypes.LONG);
			DataSet <Row> sorted = in.getDataSet().groupBy(groupColIndex).
				reduceGroup(new SortByStatCol(colIdx, groupColIndex, funcs, in.getColTypes(),
					isDescending, topN));
			this.setOutput(sorted, outputColNames, outputColTypes);
		}
		return this;
	}

	public static class SortByStatCol implements GroupReduceFunction <Row, Row> {
		private static final long serialVersionUID = -8278621481729657224L;
		private int[] colIdx;
		private int groupColIndex;
		private StatType[] funcs;
		private TypeInformation <?>[] types;
		private boolean hasAdded;
		private boolean isDesending;
		private int topN;

		public SortByStatCol(int[] colIdx, int groupColIndex,
							 StatType[] funcs, TypeInformation <?>[] types,
							 boolean isDesending, int topN) {
			this.colIdx = colIdx;
			this.groupColIndex = groupColIndex;
			this.funcs = funcs;
			this.types = types;
			this.isDesending = isDesending;
			this.topN = topN;
			this.hasAdded = funcs.length != 1;
		}

		@Override
		public void reduce(Iterable <Row> rows, Collector <Row> out) throws Exception {
			int statColNum = this.funcs.length;
			int objIndex = colIdx[0];
			Map <Object, Tuple2 <Object, List <StatCal>>> basicStats = new HashMap <>();

			for (Row row : rows) {
				Object objCol = row.getField(objIndex);
				Object groupCol = null;
				if (groupColIndex != -1) {
					groupCol = row.getField(groupColIndex);
				}
				if (basicStats.keySet().contains(objCol)) {
					for (int i = 0; i < statColNum; ++i) {
						basicStats.get(objCol).f1.get(i).add(row);
					}
				} else {
					List <StatCal> stats = new ArrayList <>();
					for (int i = 0; i < statColNum; ++i) {
						StatCal one = new StatCal(this.types[colIdx[i + 1]], colIdx[i + 1]);
						one.add(row);
						stats.add(one);
					}
					basicStats.put(objCol, new Tuple2 <>(groupCol, stats));
				}
			}

			Map <Object, Row> result = new HashMap <>();
			Object[] pre = new Object[2];
			for (Object objCol : basicStats.keySet()) {
				pre[0] = basicStats.get(objCol).f0;
				pre[1] = objCol;
				Row row = toRow(basicStats.get(objCol).f1, pre, this.funcs);
				result.put(objCol, row);
			}
			List <Map.Entry <Object, Row>> entryList = new ArrayList <>(result.entrySet());
			int statIndex = this.groupColIndex == -1 ? 1 : 2;
			entryList.sort(new StatComparator(statIndex, this.isDesending));

			Iterator <Map.Entry <Object, Row>> iter = entryList.iterator();
			Map.Entry <Object, Row> tmp = null;
			long rank = 1L;
			while (iter.hasNext() && rank <= this.topN) {
				tmp = iter.next();
				out.collect(RowUtil.merge(tmp.getValue(), rank));
				++rank;
			}
		}

		private Row toRow(List <StatCal> stats, Object[] pre, StatType[] funcs) {
			if (pre == null || pre.length < 1) {
				throw new RuntimeException("No Object col info.");
			}
			//pre[0] = groupCol, pre[1] = objCol
			int preLen = pre[0] == null ? pre.length - 1 : pre.length;
			int len = stats.size() + preLen;
			Row row = new Row(len);
			if (pre[0] == null) {
				row.setField(0, pre[1]);
			} else {
				row.setField(0, pre[0]);
				row.setField(1, pre[1]);
			}
			for (int i = preLen; i < len; ++i) {
				double value = stats.get(i - preLen).calc(funcs[i - preLen]);
				row.setField(i, value);
			}
			return row;
		}

		static class StatComparator implements Comparator <Map.Entry <Object, Row>> {
			private int index;
			private boolean isDesending;

			private StatComparator(int index, boolean isDesending) {
				this.index = index;
				this.isDesending = isDesending;
			}

			@Override
			public int compare(Map.Entry <Object, Row> o1, Map.Entry <Object, Row> o2) {
				double left = (double) o1.getValue().getField(this.index);
				double right = (double) o2.getValue().getField(this.index);
				if (left < right) {
					if (this.isDesending) {
						return 1;
					} else {
						return -1;
					}
				} else if (Math.abs(left - right) < PRECISION) {
					return 0;
				} else {
					if (this.isDesending) {
						return -1;
					} else {
						return 1;
					}
				}
			}
		}

		public class StatCal {
			private TypeInformation <?> type;
			private int colIndex;
			private long countTotal;
			private long count;
			private double sum;
			private double sum2;
			private double min;
			private double max;

			public StatCal(TypeInformation <?> type, int colIndex) {
				this.type = type;
				this.colIndex = colIndex;
				this.count = 0;
				this.countTotal = 0;
				this.sum = 0;
				this.sum2 = 0;
				this.min = Double.POSITIVE_INFINITY;
				this.max = Double.NEGATIVE_INFINITY;
			}

			public void add(Row row) {
				countTotal++;
				if (type.equals(AlinkTypes.DOUBLE) | type.equals(AlinkTypes.LONG) || type.equals(AlinkTypes.INT)) {
					if (row.getField(this.colIndex) != null) {
						double val = Double.parseDouble(row.getField(this.colIndex).toString());
						count++;
						sum += val;
						sum2 += val * val;
						max = val > max ? val : max;
						min = val < min ? val : min;
					}
				} else {
					if (row.getField(this.colIndex) != null) {
						count++;
					}
				}
			}

			public double calc(StatType statFunc) {
				switch (statFunc) {
					case count:
						return (double) count;
					case countTotal:
						return (double) countTotal;
					case min:
						return count == 0 ? 0 : min;
					case max:
						return count == 0 ? 0 : max;
					case sum:
						return sum;
					case mean:
						return count == 0 ? 0 : sum / count;
					case variance:
						if (0 == count || 1 == count || max == min) {
							return 0.0;
						} else {
							return Math.max(0.0, (sum2 - sum / count * sum) / (count - 1));
						}
					default:
						throw new RuntimeException("statFunc " + statFunc + " not support.");
				}
			}
		}
	}
}


