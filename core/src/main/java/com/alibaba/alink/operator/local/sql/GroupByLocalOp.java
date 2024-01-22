package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
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
@NameCn("SQL操作：GroupBy")
public final class GroupByLocalOp extends BaseSqlApiLocalOp <GroupByLocalOp>
	implements GroupByParams <GroupByLocalOp> {

	private static final long serialVersionUID = 8865503254860094092L;

	private static final String MATBLE_PATTERN = "mtable_agg\\(";
	private static final int MTABLE_CLAUSE_COUNT = 10;
	private final String[] mTableAggNames;

	public GroupByLocalOp() {
		this(new Params());
	}

	public GroupByLocalOp(String groupByClause, String selectClause) {
		this(new Params().set(GroupByParams.SELECT_CLAUSE, selectClause)
			.set(GroupByParams.GROUP_BY_PREDICATE, groupByClause));
	}

	public GroupByLocalOp(Params params) {
		super(params);

		mTableAggNames = new String[MTABLE_CLAUSE_COUNT];
		for (int i = 0; i < MTABLE_CLAUSE_COUNT; i++) {
			mTableAggNames[i] = "mtable_agg_" + UUID.randomUUID().toString().replace("-", "");
		}
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		String selectClause = getSelectClause();
		String groupClause = getGroupByPredicate();
		if (isHasMTableClause(selectClause)) {
			String[] groupCols = StringUtils.split(groupClause, ",");
			for (int i = 0; i < groupCols.length; i++) {
				groupCols[i] = groupCols[i].trim();
			}
			selectClause = modifyMTableClasuse(groupCols, selectClause, inputs[0].getSchema(), mTableAggNames);
		}

		this.setOutputTable(LocalMLEnvironment.getInstance().getSqlExecutor()
			.groupBy(inputs[0], groupClause, selectClause).getOutputTable());
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
			String mtableSchemaStr = TableUtil.schema2SchemaStr(new TableSchema(zippedCols, zipColTypes));

			// TODO: register function
			//MLEnvironmentFactory.get(this.getMLEnvironmentId())
			//	.getBatchTableEnvironment().registerFunction(mTableAggNames[idx],
			//		new MTableAgg(false, mtableSchemaStr));

			String newSubClause = mTableAggNames[idx]
				+ "(" + TableUtil.columnsToSqlClause(zippedCols) + ")";

			strCur = strCur.replace(subClause, newSubClause);

			idx++;
		}

		return strCur;
	}

}
