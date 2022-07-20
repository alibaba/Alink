package com.alibaba.alink.operator.common.feature.featurebuilder;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseUtil.ClauseInfo;
import com.alibaba.alink.params.feature.featuregenerator.BaseOverWindowParams;
import org.apache.commons.lang3.EnumUtils;

/**
 * This is the base stream feature builder
 * for over count window and over time window.
 */

@NameCn("")
public abstract class BaseOverWindowStreamOp<T extends BaseOverWindowStreamOp <T>>
	extends BaseWindowStreamOp <T> {

	private String intervalType;
	private String windowClause;
	private double timeInterval;

	public BaseOverWindowStreamOp(Params params) {
		super(params);
	}

	public void setWindowParams(String intervalType, String windowClause, double timeInterval) {
		this.intervalType = intervalType;
		this.windowClause = windowClause;
		this.timeInterval = timeInterval;
	}

	@Override
	Tuple2 <ClauseInfo, String> generateSqlInfo(String registerName, TableSchema inputSchema) {
		String[] reservedCols = getParams().get(BaseOverWindowParams.RESERVED_COLS);
		if (reservedCols == null) {
			reservedCols = inputColNames;
		}
		return extractSqlClause(reservedCols, registerName, inputSchema);
	}

	@Override
	String[] buildTimeCols(ClauseInfo clauseInfo, String timeCol) {
		return new String[] {timeCol};
	}

	private Tuple2 <ClauseInfo, String> extractSqlClause(String[] reservedCols, String registerName, TableSchema inputSchema) {
		String exprStr = getParams().get(BaseOverWindowParams.CLAUSE);
		String[] partitionCols = getParams().get(BaseOverWindowParams.GROUP_COLS);
		String timeCol = getParams().get(BaseOverWindowParams.TIME_COL);
		if (exprStr == null || exprStr.isEmpty()) {
			throw new AkIllegalOperatorParameterException("Please set sql clause first.");
		}
		StringBuilder sbd = new StringBuilder();
		sbd.append("select ");
		for (String reservedCol : reservedCols) {
			sbd.append(reservedCol)
				.append(", ");
		}
		sbd.append(ROW_TIME_COL_NAME)
			.append(", ");
		String[][] clauses = FeatureClauseUtil.splitClauseForMultiInput(exprStr);
		int clauseNum = clauses.length;
		ClauseInfo clauseInfo = new ClauseInfo(clauseNum);
		String[] operators = new String[clauseNum];
		String[] operatorFunc = new String[clauseNum];
		String[] asClauses = new String[clauseNum];
		for (int i = 0; i < clauseNum; i++) {
			String[] localClause = clauses[i];
			String operator = localClause[0].trim();
			FeatureClauseUtil.buildOperatorClause(operatorFunc, operators,
				i, operator, timeCol, timeInterval,
				inputSchema, MLEnvironmentFactory.get(this.getMLEnvironmentId()));
			String asClause = localClause[1].trim();
			asClauses[i] = asClause;
			clauseInfo.addClauseInfo(operator, asClause);
		}
		String partitionBy = concatColNames(partitionCols);
		StringBuilder sbdOver = new StringBuilder();
		sbdOver.append(" over (");
		if (partitionBy != null) {
			sbdOver.append("PARTITION BY ")
				.append(partitionBy);
		}
		sbdOver.append(" ORDER BY ")
			.append(ROW_TIME_COL_NAME)
			.append(" " + intervalType + " BETWEEN ")
			.append(windowClause)
			.append(" PRECEDING AND CURRENT ROW) as ");
		String overSql = sbdOver.toString();
		for (int i = 0; i < clauseNum; i++) {
			if (EnumUtils.isValidEnum(FeatureClauseOperator.class, operatorFunc[i])) {
				sbd.append(operators[i]).append(overSql).append(asClauses[i]);
			} else {
				sbd.append(operators[i]).append(" as ").append(asClauses[i]);
			}
			if (i != clauseNum - 1) {
				sbd.append(", ");
			}
		}
		sbd.append(" from ").append(registerName);

		return Tuple2.of(clauseInfo, sbd.toString());
	}

}
