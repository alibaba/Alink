package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.fe.GenerateFeatureUtil;
import com.alibaba.alink.common.fe.define.day.BaseDaysStatFeatures;
import com.alibaba.alink.common.fe.define.day.CategoricalDaysStatistics;
import com.alibaba.alink.common.fe.define.day.NDaysCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.day.NDaysNumericStatFeatures;
import com.alibaba.alink.common.fe.define.day.NumericDaysStatistics;
import com.alibaba.alink.common.fe.define.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.BaseNumericStatistics;
import com.alibaba.alink.common.fe.udaf.CatesCntUdaf;
import com.alibaba.alink.common.fe.udaf.DistinctCountUdaf;
import com.alibaba.alink.common.fe.udaf.KvCntUdaf;
import com.alibaba.alink.common.fe.udaf.MaxUdaf;
import com.alibaba.alink.common.fe.udaf.MeanUdaf;
import com.alibaba.alink.common.fe.udaf.MinUdaf;
import com.alibaba.alink.common.fe.udaf.SumUdaf;
import com.alibaba.alink.common.fe.udaf.TotalCountUdaf;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.params.feature.GenerateFeatureOfLatestDayParams;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Latest Feature Window.
 */
@NameCn("Latest特征生成")
@NameEn("Generate Feature of Latest N Days")
public class GenerateFeatureOfLatestNDaysBatchOp extends BatchOperator <GenerateFeatureOfLatestNDaysBatchOp>
	implements GenerateFeatureOfLatestDayParams <GenerateFeatureOfLatestNDaysBatchOp> {

	final static String PAST_DAYS = "past_days";
	final static String PAST_WEEKS = "past_weeks";
	final static String PAST_MONTH = "past_months";
	final static String PAST_YEARS = "past_years";
	private final String[] randomUdafNames;
	final static int MAX_UDAF_COUNT = 100;
	private static final String[] MODEL_COL_NAMES = new String[] {"group_id", "group_value", "stat_value"};
	private static final TypeInformation <?>[] MODLE_COL_TYPES = new TypeInformation <?>[] {Types.INT, Types.STRING,
		Types.STRING};
	public static final String DEFAULT_GROUP_COL = "__alink_group_col";

	private String outFeatureSchemaStr;

	public GenerateFeatureOfLatestNDaysBatchOp() {
		this(new Params());
	}

	public GenerateFeatureOfLatestNDaysBatchOp(Params params) {
		super(params);
		randomUdafNames = new String[MAX_UDAF_COUNT];
		for (int i = 0; i < MAX_UDAF_COUNT; i++) {
			randomUdafNames[i] = UUID.randomUUID().toString().replace("-", "");
		}
	}

	@Override
	public GenerateFeatureOfLatestNDaysBatchOp linkFrom(BatchOperator <?>... inputs) {
		final String timeCol = getTimeCol();
		BatchOperator <?> in = checkAndGetFirst(inputs);

		in = in
			.filter(String.format("%s <= to_timestamp('%s', 'yyyy-MM-dd')", timeCol, getBaseDate()))
			.select(
				String.format(
					"*, "
						+ "CAST(TIMESTAMPDIFF(DAY, %s, TIMESTAMP '%s 00:00:00') AS INT) AS %s,"
						+ "CAST(TIMESTAMPDIFF(WEEK, %s, TIMESTAMP '%s 00:00:00') AS INT) AS %s,"
						+ "CAST(TIMESTAMPDIFF(MONTH, %s, TIMESTAMP '%s 00:00:00') AS INT) AS %s,"
						+ "CAST(TIMESTAMPDIFF(YEAR, %s, TIMESTAMP '%s 00:00:00') AS INT) AS %s,"
						+ "1 as %s",
					timeCol, getBaseDate(), PAST_DAYS,
					timeCol, getBaseDate(), PAST_WEEKS,
					timeCol, getBaseDate(), PAST_MONTH,
					timeCol, getBaseDate(), PAST_YEARS,
					DEFAULT_GROUP_COL)
			);

		//step 1: merge stat features by group cols;
		BaseDaysStatFeatures <?>[] featureArrays = BaseDaysStatFeatures.fromJson(getFeatureDefinitions(),
			BaseDaysStatFeatures[].class);
		Map <String[], List <BaseDaysStatFeatures <?>>> mergedFeatures =
			GenerateFeatureUtil.mergeFeatures(featureArrays);

		List <BatchOperator <?>> statModelOps = new ArrayList <>();
		List <Table> statOps = new ArrayList <>();

		//step2: split extend exprs.
		List <String[]> featureColsWithoutExtendCols = new ArrayList <>();
		for (Map.Entry <String[], List <BaseDaysStatFeatures <?>>> entry :
			mergedFeatures.entrySet()) {
			featureColsWithoutExtendCols.add(
				getStatSchema(entry.getValue().toArray(new BaseDaysStatFeatures[0]), in.getSchema(),
					new HashMap <>()).getFieldNames());
		}

		Map <Integer, String> extendFeatureMaps = getExtendClause(getExtendFeatures(), featureColsWithoutExtendCols);
		Map <Integer, TableSchema> extendSchemas = new HashMap <>();

		int fromIdx = 0;
		int groupIdx = 0;
		for (Map.Entry <String[], List <BaseDaysStatFeatures <?>>> entry :
			mergedFeatures.entrySet()) {
			String[] groupCols = entry.getKey();
			List <BaseDaysStatFeatures <?>> features = entry.getValue();
			BaseDaysStatFeatures <?>[] featuresArray = features.toArray(new BaseDaysStatFeatures <?>[0]);

			BatchOperator <?> inForStat = in;

			Tuple2 <String, Integer> tuple2 = getClauseAndRegisterUdaf(features, inForStat.getSchema(),
				inForStat.getMLEnvironmentId(), fromIdx, this.randomUdafNames);

			int groupColNum = groupCols.length;

			BatchOperator <?> group = new TableSourceBatchOp(DataSetConversionUtil.toTable(
				inForStat.getMLEnvironmentId(),
				inForStat.groupBy(String.join(",", groupCols), tuple2.f0)
					.getDataSet()
					.map(new ExtendFeatureMap(groupColNum)),
				getSchemaWithGroupCols(featuresArray, in.getSchema())
			));

			if (extendFeatureMaps != null) {
				if (extendFeatureMaps.containsKey(groupIdx)) {
					TableSchema oriSchema = group.getSchema();
					group = group.select(String.format("%s, %s",
						String.join(",", group.getColNames()),
						extendFeatureMaps.get(groupIdx)));
					extendSchemas.put(groupIdx, excludeSchema(group.getSchema(), oriSchema));
				}
			}

			statOps.add(group.getOutputTable());
			fromIdx = tuple2.f1;

			statModelOps.add(new TableSourceBatchOp(
				DataSetConversionUtil.toTable(inForStat.getMLEnvironmentId(), group
						.getDataSet()
						.map(new BuildSingleModelMap(groupIdx + 1, groupColNum)), MODEL_COL_NAMES,
					MODLE_COL_TYPES))
			);

			groupIdx++;
		}

		//write meta.
		int numGroups = mergedFeatures.size();
		String[][] groupColsArray = new String[numGroups][];
		int[] valueLens = new int[numGroups];

		int idx = 0;
		for (Map.Entry <String[], List <BaseDaysStatFeatures <?>>> entry : mergedFeatures.entrySet()) {
			groupColsArray[idx] = entry.getKey();
			valueLens[idx] = entry.getKey().length;
			idx++;
		}

		this.outFeatureSchemaStr = getSchemaStr(featureArrays, in.getSchema(), extendSchemas);

		Params p = new Params()
			.set("featureSchemaStr", getSchemaStr(featureArrays, in.getSchema(), extendSchemas))
			.set("numGroups", mergedFeatures.size())
			.set("groupColsArray", groupColsArray)
			.set("valueLens", valueLens);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			new Row[] {Row.of(0, p.toJson(), "")}, new TableSchema(MODEL_COL_NAMES, MODLE_COL_TYPES)
		).setMLEnvironmentId(in.getMLEnvironmentId());

		statModelOps.add(memSourceBatchOp);

		BatchOperator <?> result = new UnionAllBatchOp()
			.setMLEnvironmentId(in.getMLEnvironmentId())
			.linkFrom(statModelOps);

		this.setOutputTable(result.getOutputTable());
		this.setSideOutputTables(statOps.toArray(new Table[0]));

		return this;
	}

	public String getOutFeatureSchemaStr() {
		return this.outFeatureSchemaStr;
	}

	//find col in schema1 not in schema2
	static TableSchema excludeSchema(TableSchema schema1, TableSchema schema2) {
		List <String> cols = new ArrayList <>();
		String[] cols1 = schema1.getFieldNames();
		for (int i = 0; i < cols1.length; i++) {
			if (TableUtil.findColIndex(schema2, cols1[i]) == -1) {
				cols.add(cols1[i]);
			}
		}
		return new TableSchema(cols.toArray(new String[0]),
			TableUtil.findColTypes(schema1, cols.toArray(new String[0])));
	}

	//Determine which group the expression belongs to, and combine the exprs in the same group.
	static Map <Integer, String> getExtendClause(String extendFeatures, List <String[]> featureCols) {
		if (extendFeatures == null || extendFeatures.length() == 0) {
			return null;
		}
		String[] clauses = StringUtils.split(extendFeatures, ",");
		//find group idx.
		int[] groupIndices = new int[clauses.length];
		for (int i = 0; i < clauses.length; i++) {
			String clause = clauses[i];
			boolean isFind = false;
			for (int j = 0; j < featureCols.size() && !isFind; j++) {
				for (String col : featureCols.get(j)) {
					int idx = clause.indexOf(col);
					if (idx == -1) {
						continue;
					}
					if (!check(clause, idx + col.length())) {
						groupIndices[i] = j;
						isFind = true;
						break;
					}
				}
			}
			if (!isFind) {
				StringBuilder sbd = new StringBuilder();
				for (String[] featureCol : featureCols) {
					sbd.append(String.join(",", featureCol))
						.append(",");
				}
				String colStr = "";
				if (sbd.length() > 1) {
					colStr = sbd.substring(0, sbd.length() - 1);
				}
				throw new AkIllegalOperatorParameterException(
					"col in " + extendFeatures + " is not exist. maybe " + colStr);
			}
		}
		//combine exprs
		Map <Integer, List <String>> maps = new HashMap <>();
		for (int i = 0; i < groupIndices.length; i++) {
			if (maps.containsKey(groupIndices[i])) {
				maps.get(groupIndices[i]).add(clauses[i]);
			} else {
				List <String> stringList = new ArrayList <>();
				stringList.add(clauses[i]);
				maps.put(groupIndices[i], stringList);
			}
		}

		Map <Integer, String> reMaps = new HashMap <>();
		for (Map.Entry <Integer, List <String>> entry : maps.entrySet()) {
			reMaps.put(entry.getKey(), String.join(",", entry.getValue().toArray(new String[0])));
		}

		return reMaps;
	}

	public static boolean check(String data, int idx) {
		char c = data.charAt(idx);
		if (c >= 'a' && c <= 'z') {
			return true;
		}
		if (c >= 'A' && c <= 'Z') {
			return true;
		}
		if (Character.isDigit(c)) {
			return true;
		}
		return c == '_';
	}

	public static class ExtendFeatureMap extends RichMapFunction <Row, Row> {
		private int groupColNum;

		public ExtendFeatureMap(int groupColNum) {
			this.groupColNum = groupColNum;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public Row map(Row value) throws Exception {
			List <Object> stats = new ArrayList <>();
			for (int i = groupColNum; i < value.getArity(); i++) {
				Object stat = value.getField(i);
				if (stat instanceof ArrayList) {
					stats.addAll((ArrayList) stat);
				} else {
					stats.add(stat);
				}
			}
			Row out = new Row(groupColNum + stats.size());
			for (int i = 0; i < groupColNum; i++) {
				out.setField(i, value.getField(i));
			}
			int idx = 0;
			for (Object stat : stats) {
				out.setField(groupColNum + idx++, stat);
			}
			return out;
		}
	}

	public static class BuildSingleModelMap extends RichMapFunction <Row, Row> {
		private int id;
		private int groupColNum;
		Encoder encoder;

		public BuildSingleModelMap(int id, int groupColNum) {
			this.id = id;
			this.groupColNum = groupColNum;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.encoder = Base64.getEncoder();
		}

		@Override
		public Row map(Row value) throws Exception {
			Object[] groupValues = new Object[groupColNum];
			for (int i = 0; i < groupColNum; i++) {
				groupValues[i] = value.getField(i);
			}
			Object[] stats = new Object[value.getArity() - groupColNum];
			for (int i = groupColNum; i < value.getArity(); i++) {
				stats[i - groupColNum] = value.getField(i);
			}
			return Row.of(this.id,
				encoder.encodeToString(SerializationUtils.serialize(groupValues)),
				encoder.encodeToString(SerializationUtils.serialize(stats))
			);
		}
	}

	public static class SingleFeatureMap extends RichMapFunction <Row, Row> {
		private int id;
		private int groupColNum;
		Encoder encoder;

		public SingleFeatureMap(int id, int groupColNum) {
			this.id = id;
			this.groupColNum = groupColNum;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.encoder = Base64.getEncoder();
		}

		@Override
		public Row map(Row value) throws Exception {
			Object[] groupValues = new Object[groupColNum];
			for (int i = 0; i < groupColNum; i++) {
				groupValues[i] = value.getField(i);
			}
			List <Object> stats = new ArrayList <>();
			for (int i = groupColNum; i < value.getArity(); i++) {
				Object stat = value.getField(i);
				if (stat instanceof ArrayList) {
					stats.addAll((ArrayList) stat);
				} else {
					stats.add(stat);
				}
			}
			return Row.of(this.id,
				encoder.encodeToString(SerializationUtils.serialize(groupValues)),
				encoder.encodeToString(SerializationUtils.serialize(stats.toArray(new Object[0])))
			);
		}

	}

	static Tuple2 <String, Integer> getClauseAndRegisterUdaf(List <BaseDaysStatFeatures <?>> features,
															 TableSchema schema,
															 long envId,
															 int fromIdx,
															 String[] randomNames) {
		BatchTableEnvironment bEnv = MLEnvironmentFactory.get(envId).getBatchTableEnvironment();
		StringBuilder sbd = new StringBuilder();
		int idx = fromIdx;

		sbd.append(String.join(",", features.get(0).groupCols));
		sbd.append(",");

		for (BaseDaysStatFeatures <?> feature : features) {
			if (feature instanceof NDaysNumericStatFeatures) {
				BaseNumericStatistics[] statistics = ((NDaysNumericStatFeatures) feature).getNumericStatistics();
				String[] colNames = ((NDaysNumericStatFeatures) feature).getFeatureCols();
				String[] nDays = feature.getNDays();
				String[][] conditions = feature.getConditions();
				for (BaseNumericStatistics statFunc : statistics) {
					String udafName = String.join("_", statFunc.name(), randomNames[idx]);
					if (NumericDaysStatistics.SUM == statFunc) {
						bEnv.registerFunction(udafName, new SumUdaf(nDays, colNames.length, conditions));
					} else if (NumericDaysStatistics.TOTAL_COUNT == statFunc) {
						bEnv.registerFunction(udafName, new TotalCountUdaf(nDays, colNames.length, conditions, true));
					} else if (NumericDaysStatistics.COUNT == statFunc) {
						bEnv.registerFunction(udafName, new TotalCountUdaf(nDays, colNames.length, conditions, false));
					} else if (NumericDaysStatistics.MIN == statFunc) {
						bEnv.registerFunction(udafName, new MinUdaf(nDays, colNames.length, conditions));
					} else if (NumericDaysStatistics.MAX == statFunc) {
						bEnv.registerFunction(udafName, new MaxUdaf(nDays, colNames.length, conditions));
					} else if (NumericDaysStatistics.MEAN == statFunc) {
						bEnv.registerFunction(udafName, new MeanUdaf(nDays, colNames.length, conditions));
					} else {
						throw new AkIllegalOperatorParameterException(
							String.format("stat function [%s] not support yet.", statFunc));
					}
					if (conditions == null) {
						String subClause = String.format("%s(%s, %s, %s, %s, %s)",
							udafName,
							StringUtils.join(colNames, ','),
							PAST_DAYS,
							PAST_WEEKS,
							PAST_MONTH,
							PAST_YEARS);
						sbd.append(subClause).append(",");
					} else {
						String subClause = String.format("%s(%s, %s, %s, %s, %s, %s)",
							udafName,
							StringUtils.join(colNames, ','),
							PAST_DAYS,
							PAST_WEEKS,
							PAST_MONTH,
							PAST_YEARS,
							feature.getConditionCol());
						sbd.append(subClause).append(",");
					}
					idx++;
				}
			} else if (feature instanceof NDaysCategoricalStatFeatures) {
				BaseCategoricalStatistics[] statistics
					= ((NDaysCategoricalStatFeatures) feature).getCategoricalStatistics();
				String[] colNames = ((NDaysCategoricalStatFeatures) feature).getFeatureCols();
				String[] nDays = feature.getNDays();
				String[][] conditions = feature.getConditions();
				for (BaseCategoricalStatistics statFunc : statistics) {
					String udafName = String.join("_", statFunc.name(), randomNames[idx]);
					if (CategoricalDaysStatistics.CATES_CNT == statFunc) {
						bEnv.registerFunction(udafName,
							new CatesCntUdaf(((NDaysCategoricalStatFeatures) feature).featureItems,
								nDays, conditions));
					} else if (CategoricalDaysStatistics.KV_CNT == statFunc) {
						bEnv.registerFunction(udafName,
							new KvCntUdaf(colNames.length, nDays, conditions));
					} else if (CategoricalDaysStatistics.TOTAL_COUNT == statFunc) {
						bEnv.registerFunction(udafName,
							new TotalCountUdaf(nDays, colNames.length, conditions, true));
					} else if (CategoricalDaysStatistics.COUNT == statFunc) {
						bEnv.registerFunction(udafName,
							new TotalCountUdaf(nDays, colNames.length, conditions, false));
					} else if (CategoricalDaysStatistics.DISTINCT_COUNT == statFunc) {
						bEnv.registerFunction(udafName, new DistinctCountUdaf(nDays, colNames.length, conditions));
					} else {
						throw new AkIllegalOperatorParameterException(
							String.format("stat function [%s] not support yet.", statFunc));
					}
					if (conditions == null) {
						String subClause = String.format("%s(%s, %s, %s, %s, %s)",
							udafName,
							StringUtils.join(colNames, ','),
							PAST_DAYS,
							PAST_WEEKS,
							PAST_MONTH,
							PAST_YEARS);
						sbd.append(subClause).append(",");
					} else {
						String subClause = String.format("%s(%s, %s, %s, %s, %s, %s)",
							udafName,
							StringUtils.join(colNames, ','),
							PAST_DAYS,
							PAST_WEEKS,
							PAST_MONTH,
							PAST_YEARS,
							feature.getConditionCol());
						sbd.append(subClause).append(",");
					}
					idx++;
				}
			}
		}

		return Tuple2.of(sbd.substring(0, sbd.length() - 1), idx);
	}

	public static TableSchema getStatSchema(BaseDaysStatFeatures <?>[] features, TableSchema inputSchema,
											Map <Integer, TableSchema> extendSchemas) {
		Map <String[], List <BaseDaysStatFeatures <?>>> mergedFeatures = GenerateFeatureUtil.mergeFeatures(features);
		String[] allStatColNames = new String[0];
		TypeInformation <?>[] allStatColTypes = new TypeInformation <?>[0];
		int idx = 0;
		for (Map.Entry <String[], List <BaseDaysStatFeatures <?>>> entry : mergedFeatures.entrySet()) {
			for (BaseDaysStatFeatures <?> feature : entry.getValue()) {
				allStatColNames = GenerateFeatureUtil.mergeColNames(allStatColNames, feature.getOutColNames());
				allStatColTypes = GenerateFeatureUtil.mergeColTypes(allStatColTypes,
					feature.getOutColTypes(inputSchema));
			}
			if (extendSchemas != null && extendSchemas.containsKey(idx)) {
				allStatColNames = GenerateFeatureUtil.mergeColNames(allStatColNames,
					extendSchemas.get(idx).getFieldNames());
				allStatColTypes = GenerateFeatureUtil.mergeColTypes(allStatColTypes,
					extendSchemas.get(idx).getFieldTypes());
			}
			idx++;
		}

		return new TableSchema(allStatColNames, allStatColTypes);
	}

	public static String getSchemaStr(BaseDaysStatFeatures <?>[] features, TableSchema inputSchema) {
		return TableUtil.schema2SchemaStr(getStatSchema(features, inputSchema, new HashMap <>()));
	}

	public static String getSchemaStr(BaseDaysStatFeatures <?>[] features, String extendFeatures,
									  TableSchema inputSchema) {
		return TableUtil.schema2SchemaStr(getStatSchema(features, inputSchema, new HashMap <>()));
	}

	public static String getSchemaStr(BaseDaysStatFeatures <?>[] features, TableSchema inputSchema,
									  Map <Integer, TableSchema> extendSchemas) {
		return TableUtil.schema2SchemaStr(getStatSchema(features, inputSchema, extendSchemas));
	}

	public static TableSchema getSchemaWithGroupCols(BaseDaysStatFeatures <?>[] features, TableSchema inputSchema) {
		TableSchema schema = getStatSchema(features, inputSchema, new HashMap <>());

		String[] allStatColNames = features[0].groupCols;
		TypeInformation <?>[] allStatColTypes = TableUtil.findColTypes(inputSchema, allStatColNames);

		allStatColNames = GenerateFeatureUtil.mergeColNames(allStatColNames, schema.getFieldNames());
		allStatColTypes = GenerateFeatureUtil.mergeColTypes(allStatColTypes, schema.getFieldTypes());

		return new TableSchema(allStatColNames, allStatColTypes);
	}

}

