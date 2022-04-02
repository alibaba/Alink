package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.featurebuilder.DateUtil;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseUtil;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseUtil.ClauseInfo;
import com.alibaba.alink.params.feature.featuregenerator.GroupTimeWindowParams;
import com.alibaba.alink.params.feature.featuregenerator.HopTimeWindowParams;
import com.alibaba.alink.params.feature.featuregenerator.SessionTimeWindowParams;
import com.alibaba.alink.params.feature.featuregenerator.TumbleTimeWindowParams;

import java.util.ArrayList;

/**
 * This is the base stream feature builder for
 * tumble time window, hop time window and session time window.
 */
@NameCn("")
abstract class BaseGroupTimeWindowStreamOp<T extends BaseGroupTimeWindowStreamOp <T>>
	extends BaseWindowStreamOp <T> implements GroupTimeWindowParams <T> {

	final GroupWindowType windowType;

	public BaseGroupTimeWindowStreamOp(Params params, GroupWindowType windowType) {
		super(params);
		this.windowType = windowType;
	}

	@Override
	Tuple2 <ClauseInfo, String> generateSqlInfo(String registerName, TableSchema inputSchema) {
		return extractSqlClause(registerName, inputSchema);
	}

	@Override
	void generateWindowClause() {}

	@Override
	String[] buildTimeCols(ClauseInfo clauseInfo, String timeCol) {
		return clauseInfo.getGroupWindowTimeCol();
	}

	private Tuple2 <ClauseInfo, String> extractSqlClause(String registerName, TableSchema inputSchema) {

		String timeCol = getTimeCol();
		String clauseInterval = null;
		switch (this.windowType) {
			case TUMBLE:
				clauseInterval = ROW_TIME_COL_NAME + ", INTERVAL "
					+ DateUtil.parseSecondTime(getParams().get(TumbleTimeWindowParams.WINDOW_TIME));
				break;
			case HOP:
				clauseInterval = ROW_TIME_COL_NAME + ", INTERVAL "
					+ DateUtil.parseSecondTime(getParams().get(HopTimeWindowParams.HOP_TIME))
					+ ", INTERVAL "
					+ DateUtil.parseSecondTime(getParams().get(HopTimeWindowParams.WINDOW_TIME));
				break;
			case SESSION:
				clauseInterval = ROW_TIME_COL_NAME + ", INTERVAL "
					+ DateUtil.parseSecondTime(getParams().get(SessionTimeWindowParams.SESSION_GAP_TIME));
				break;
			default:
				throw new IllegalArgumentException("Not support this type : " + this.windowType);
		}

		ArrayList <String> partitionCols = new ArrayList <>();
		for (String s : getPartitionCols()) {
			if (null != s && s.trim().length() > 0 && !partitionCols.contains(s.trim())) {
				partitionCols.add(s.trim());
			}
		}

		StringBuilder sbd = new StringBuilder();
		sbd.append("SELECT ");
		for (String partitionCol : partitionCols) {
			sbd.append(partitionCol).append(", ");
		}
		String exprStr = getClause();
		if (exprStr == null || exprStr.isEmpty()) {
			throw new RuntimeException("expressions must be set");
		}
		String[][] clauses = FeatureClauseUtil.splitClauseForMultiInput(exprStr);
		int clauseNum = clauses.length;
		ClauseInfo clauseInfo = new ClauseInfo(clauseNum);
		String[] operators = new String[clauseNum];
		String[] operatorFunc = new String[clauseNum];
		String[] asClauses = new String[clauseNum];
		double timeInterval = -1;
		for (int i = 0; i < clauseNum; i++) {
			String[] localClause = clauses[i];
			String operator = localClause[0].trim();
			FeatureClauseUtil.buildOperatorClause(operatorFunc, operators,
				i, operator, ROW_TIME_COL_NAME, timeInterval,
				inputSchema, MLEnvironmentFactory.get(this.getMLEnvironmentId()));
			String operaFunc = operatorFunc[i];
			switch (this.windowType) {
				case TUMBLE:
					if (operaFunc.equals("TUMBLE_START") || operaFunc.equals("TUMBLE_END")
						|| operaFunc.equals("TUMBLE_ROWTIME") || operaFunc.equals("")) {
						operators[i] = operaFunc + "(" + clauseInterval + ")";
					}
					break;
				case HOP:
					if (operaFunc.equals("HOP_START") || operaFunc.equals("HOP_END")
						|| operaFunc.equals("HOP_ROWTIME") || operaFunc.equals("HOP_PROCTIME")) {
						operators[i] = operaFunc + "(" + clauseInterval + ")";
					}
					break;
				case SESSION:
					if (operaFunc.equals("SESSION_START") || operaFunc.equals("SESSION_END")
						|| operaFunc.equals("SESSION_ROWTIME") || operaFunc.equals("SESSION_PROCTIME")) {
						operators[i] = operaFunc + "(" + clauseInterval + ")";
					}
					break;
			}
			String asClause = localClause[1].trim();
			asClauses[i] = asClause;
			clauseInfo.addClauseInfo(operator, asClause);
		}
		for (int i = 0; i < clauseNum; i++) {
			sbd.append(operators[i]).append(" AS ").append(asClauses[i]);
			if (i != clauseNum - 1) {
				sbd.append(", ");
			}
		}
		sbd.append(" FROM ").append(registerName)
			.append(" GROUP BY ").append(this.windowType)
			.append("(").append(clauseInterval).append(")");
		if (partitionCols.size() != 0) {
			for (String partitionCol : partitionCols) {
				sbd.append(", ").append(partitionCol);
			}
		}
		return Tuple2.of(clauseInfo, sbd.toString());
	}

}
