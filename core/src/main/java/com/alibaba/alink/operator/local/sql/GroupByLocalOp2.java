package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;
import com.alibaba.alink.common.sql.builtin.agg.CountUdaf;
import com.alibaba.alink.common.sql.builtin.agg.ListAggUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MTableAgg;
import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClause;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseUtil;
import com.alibaba.alink.operator.common.feature.featurebuilder.WindowResColType;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.GroupByParams;
import com.alibaba.alink.params.statistics.HasIsSingleThread;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Apply the "group by" operation on the input batch operator.
 */
@NameCn("SQL操作：GroupBy")
public final class GroupByLocalOp2 extends BaseSqlApiLocalOp <GroupByLocalOp2>
	implements GroupByParams <GroupByLocalOp2> {

	static SortUtils.ComparableComparator comparator = new SortUtils.ComparableComparator();

	public GroupByLocalOp2() {
		this(new Params());
	}

	public GroupByLocalOp2(String groupByClause, String selectClause) {
		this(new Params().set(GroupByParams.SELECT_CLAUSE, selectClause)
			.set(GroupByParams.GROUP_BY_PREDICATE, groupByClause));
	}

	public GroupByLocalOp2(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = inputs[0];

		// get group cols.
		String[] groupCols = StringUtils.split(getGroupByPredicate(), ",");
		for (int i = 0; i < groupCols.length; i++) {
			groupCols[i] = groupCols[i].replace("`", "").trim();
		}

		int[] groupIndices = TableUtil.findColIndices(in.getSchema(), groupCols);

		// parse clause.
		String selectClause = this.getSelectClause();
		String[] inputColNames = in.getColNames();
		FeatureClause[] featureClauses = FeatureClauseUtil.extractFeatureClauses(
			selectClause, in.getSchema(), null);

		// get output schema.
		String[] outColNames = new String[featureClauses.length];
		TypeInformation <?>[] outColTypes = new TypeInformation[featureClauses.length];
		for (int i = 0; i < featureClauses.length; i++) {
			TableUtil.assertSelectedColExist(in.getColNames(), featureClauses[i].inColName);
			outColNames[i] = featureClauses[i].outColName;
			if (featureClauses[i].op == null ||
				WindowResColType.RES_TYPE.equals(featureClauses[i].op.getResType())) {
				outColTypes[i] = TableUtil.findColType(in.getSchema(),
					featureClauses[i].inColName);
			} else {
				outColTypes[i] = featureClauses[i].op.getResType();
			}
		}

		// if output cols duplicate.
		HashSet <String> sets = new HashSet <String>(Arrays.asList(outColNames));
		if (sets.size() != outColNames.length) {
			throw new AkIllegalOperatorParameterException("output col names ard duplicated.");
		}

		MTable mTable = in.getOutputTable();
		List <Row> outRows = new ArrayList <>();

		// deal with empty table.
		if (mTable.getNumRow() == 0) {
			this.setOutputTable(new MTable(outRows, outColNames, outColTypes));
			return;
		}

		// type 3 result error when parallelism > 1;
		int threadNum = getParallelism();
		if (getParams().contains(HasIsSingleThread.THREAD_NUM)) {
			threadNum = getParams().get(HasIsSingleThread.THREAD_NUM);
		}

		final List <Row> input = in.getOutputTable().getRows();
		final int numRows = input.size();
		Map <Row, BaseUdaf <?, ?>[]> calcMaps = new TreeMap <>(new RowComparator(groupIndices)::compare);

		if (threadNum == 1) {
			List <Object[]> aggInputDataList = new ArrayList <>();
			for (FeatureClause featureClause : featureClauses) {
				aggInputDataList.add(new Object[featureClause.inputParams.length + 1]);
			}

			for (Row row : mTable.getRows()) {
				try {
					calcMaps.compute(row, (k, v) -> {
						if (null == v) {
							v = newUdafs(featureClauses);
						}
						calc(featureClauses, v, inputColNames, k, aggInputDataList);
						return v;
					});
				} catch (Exception e) {
					e.printStackTrace();
					throw new AkIllegalDataException(e.getMessage());
				}
			}
		} else {
			final TaskRunner taskRunner = new TaskRunner();

			Map <Row, BaseUdaf <?, ?>[]>[] calcMapsList = new Map[threadNum];
			for (int i = 0; i < threadNum; i++) {
				calcMapsList[i] = new TreeMap <>(new RowComparator(groupIndices)::compare);
			}

			for (int i = 0; i < threadNum; ++i) {
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, threadNum, numRows);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, threadNum, numRows);

				if (cnt <= 0) {continue;}

				int finalI = i;
				taskRunner.submit(() -> {
					List <Object[]> aggInputDataList = new ArrayList <>();
					for (FeatureClause featureClause : featureClauses) {
						aggInputDataList.add(new Object[featureClause.inputParams.length + 1]);
					}
					for (int j = start; j < Math.min(start + cnt, numRows); j++) {
						try {
							calcMapsList[finalI].compute(input.get(j), (k, v) -> {
								if (null == v) {
									v = newUdafs(featureClauses);
								}
								calc(featureClauses, v, inputColNames, k, aggInputDataList);
								return v;
							});
						} catch (Exception e) {
							e.printStackTrace();
							throw new AkIllegalDataException(e.getMessage());

						}
					}
				});
			}

			taskRunner.join();

			for (Map <Row, BaseUdaf <?, ?>[]> rowMap : calcMapsList) {
				for (Map.Entry <Row, BaseUdaf <?, ?>[]> entry : rowMap.entrySet()) {
					Row key = entry.getKey();
					BaseUdaf <?, ?>[] udafLeft = entry.getValue();
					if (calcMaps.containsKey(key)) {
						BaseUdaf <?, ?>[] udafRight = calcMaps.get(key);
						for (int j = 0; j < udafLeft.length; j++) {
							if (udafLeft[j] != null) {
								udafLeft[j].merge(udafRight[j]);
							}
						}
						calcMaps.put(key, udafLeft);
					} else {
						calcMaps.put(key, udafLeft);
					}
				}
			}
		}
		for (Map.Entry <Row, BaseUdaf <?, ?>[]> entry : calcMaps.entrySet()) {
			outRows.add(getOutRow(featureClauses, entry.getValue(), entry.getKey(), in.getSchema(), outColTypes));
		}
		this.setOutputTable(new MTable(outRows, outColNames, outColTypes));
	}

	public static class RowComparator implements Comparator <Row> {
		private final SortUtils.ComparableComparator objectComparator = new SortUtils.ComparableComparator();
		private final int[] indices;

		public RowComparator(int[] indices) {
			this.indices = indices;
		}

		@Override
		public int compare(Row o1, Row o2) {
			for (int idx : indices) {
				int compare = objectComparator.compare(o1.getField(idx), o2.getField(idx));
				if (compare != 0) {
					return compare;
				}
			}
			return 0;
		}
	}

	// aggInputDataList reserved.
	private void calc(FeatureClause[] featureClauses,
					  BaseUdaf <?, ?>[] calcs,
					  String[] inputColNames,
					  Row value,
					  List <Object[]> aggInputDataList
	) {
		for (int i = 0; i < featureClauses.length; i++) {
			BaseUdaf <?, ?> udaf = calcs[i];
			if (udaf == null) {
			} else if (udaf instanceof CountUdaf) {
				udaf.accumulateBatch(0);
			} else if (udaf instanceof MTableAgg) {
				Object[] aggInputData = aggInputDataList.get(i);
				aggInputData[0] = value.getField(
					TableUtil.findColIndex(inputColNames, featureClauses[i].inColName));
				for (int j = 0; j < featureClauses[i].inputParams.length; j++) {
					aggInputData[1 + j] = value.getField(
						TableUtil.findColIndex(inputColNames, (String) featureClauses[i].inputParams[j]));
				}

				udaf.accumulateBatch(aggInputData);
			} else {
				Object[] aggInputData = aggInputDataList.get(i);
				aggInputData[0] = value.getField(
					TableUtil.findColIndex(inputColNames, featureClauses[i].inColName));
				System.arraycopy(featureClauses[i].inputParams, 0, aggInputData, 1,
					featureClauses[i].inputParams.length);

				if (!(udaf instanceof ListAggUdaf)) {
					for (int i1 = 1; i1 < aggInputData.length; i1++) {
						aggInputData[i1] = Integer.parseInt((String) aggInputData[i1]);
					}
				}
				udaf.accumulateBatch(aggInputData);
			}
		}
	}

	private Row getOutRow(FeatureClause[] featureClauses,
						  BaseUdaf <?, ?>[] calcs,
						  Row firstRowOfGroup,
						  TableSchema schema,
						  TypeInformation <?>[] outSchema) {
		Row out = new Row(featureClauses.length);
		for (int i = 0; i < featureClauses.length; i++) {
			if (calcs[i] == null) {
				out.setField(i, firstRowOfGroup.getField(
					TableUtil.findColIndex(schema, featureClauses[i].inColName)));
			} else {
				Object o = calcs[i].getValueBatch();
				out.setField(i, o);
				if (o != null) {
					if (o instanceof Double && outSchema[i] == AlinkTypes.INT) {
						out.setField(i, ((Number) o).intValue());
					}
				}
			}
		}
		return out;
	}

	private BaseUdaf <?, ?>[] newUdafs(FeatureClause[] featureClauses) {
		BaseUdaf <?, ?>[] calcs = new BaseUdaf[featureClauses.length];
		for (int i = 0; i < featureClauses.length; i++) {
			if (featureClauses[i].op != null) {
				calcs[i] = featureClauses[i].op.getCalc();
			} else {
				calcs[i] = null;
			}
		}
		return calcs;
	}

	private boolean compare(Row left, Row right, int[] indices) {
		for (int idx : indices) {
			if (0 != comparator.compare(left.getField(idx), right.getField(idx))) {
				return false;
			}
		}
		return true;
	}

}
