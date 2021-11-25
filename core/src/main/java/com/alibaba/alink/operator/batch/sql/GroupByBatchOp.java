package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.sql.builtin.agg.MTableAgg;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.sql.GroupByParams;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Apply the "group by" operation on the input batch operator.
 */
public final class GroupByBatchOp extends BaseSqlApiBatchOp <GroupByBatchOp>
	implements GroupByParams <GroupByBatchOp> {

	private static final long serialVersionUID = 8865503254860094092L;

	private static final String MATBLE_PATTERN = "mtable_agg\\(";
	private static final int MTABLE_CLAUSE_COUNT = 10;
	private String[] mTableAggNames;

	public GroupByBatchOp() {
		this(new Params());
	}

	public GroupByBatchOp(String groupByClause, String selectClause) {
		this(new Params().set(GroupByParams.SELECT_CLAUSE, selectClause)
			.set(GroupByParams.GROUP_BY_PREDICATE, groupByClause));
	}

	public GroupByBatchOp(Params params) {
		super(params);

		mTableAggNames = new String[MTABLE_CLAUSE_COUNT];
		for (int i = 0; i < MTABLE_CLAUSE_COUNT; i++) {
			mTableAggNames[i] = "mtable_agg_" + UUID.randomUUID().toString().replace("-", "");
		}
	}

	@Override
	public GroupByBatchOp linkFrom(BatchOperator <?>... inputs) {
		String selectClause = getSelectClause();
		String groupClause = getGroupByPredicate();
		if (isHasMTableClause(selectClause)) {
			String[] groupCols = StringUtils.split(groupClause, ",");
			for (int i = 0; i < groupCols.length; i++) {
				groupCols[i] = groupCols[i].trim();
			}
			selectClause = modifyMTableClasuse(groupCols, selectClause, inputs[0].getSchema(), mTableAggNames);
		}

		this.setOutputTable(inputs[0].groupBy(groupClause, selectClause).getOutputTable());
		return this;
	}

	static boolean isHasMTableClause(String clause) {
		Pattern p = Pattern.compile(MATBLE_PATTERN, Pattern.CASE_INSENSITIVE);
		Matcher match = p.matcher(clause);
		return match.find();
	}

	String modifyMTableClasuse(String[] groupCols,
							   String clause,
							   TableSchema tableSchema,
							   String[] mTableAggNames) {
		Pattern p = Pattern.compile(MATBLE_PATTERN, Pattern.CASE_INSENSITIVE);
		String strCur = clause;

		Matcher match = p.matcher(clause);
		int idx = 0;
		while (match.find()) {
			int subClauseStart = match.start();
			int funcEnd = match.end();
			int subClauseEnd = clause.indexOf(")", funcEnd);

			String subClause = clause.substring(subClauseStart, subClauseEnd + 1);

			String[] zippedCols = StringUtils.split(clause.substring(funcEnd, subClauseEnd), ",");
			for (int i = 0; i < zippedCols.length; i++) {
				zippedCols[i] = zippedCols[i].trim();
			}

			if (0 == zippedCols.length) {
				List <String> zippedColsList = new ArrayList <>();
				for (String col : tableSchema.getFieldNames()) {
					if (TableUtil.findColIndex(groupCols, col) == -1) {
						zippedColsList.add(col);
					}
				}
				zippedCols = zippedColsList.toArray(new String[0]);
			}
			TypeInformation <?>[] zipColTypes = TableUtil.findColTypes(tableSchema, zippedCols);
			String mtableSchemaStr = CsvUtil.schema2SchemaStr(new TableSchema(zippedCols, zipColTypes));

			MLEnvironmentFactory.get(this.getMLEnvironmentId())
				.getBatchTableEnvironment().registerFunction(mTableAggNames[idx],
					new MTableAgg(false, mtableSchemaStr));

			String newSubClause = mTableAggNames[idx]
				+ "(" + TableUtil.columnsToSqlClause(zippedCols) + ")";

			strCur = strCur.replace(subClause, newSubClause);

			idx++;
		}

		return strCur;
	}

}
