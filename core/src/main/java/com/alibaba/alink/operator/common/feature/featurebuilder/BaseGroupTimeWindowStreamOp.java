package com.alibaba.alink.operator.common.feature.featurebuilder;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
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
public abstract class BaseGroupTimeWindowStreamOp<T extends BaseGroupTimeWindowStreamOp <T>>
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
	public void generateWindowClause() {}

	@Override
	String[] buildTimeCols(ClauseInfo clauseInfo, String timeCol) {
		return clauseInfo.getGroupWindowTimeCol();
	}

	private Tuple2 <ClauseInfo, String> extractSqlClause(String registerName, TableSchema inputSchema) {

		String clauseInterval = null;

		switch (this.windowType) {
			case TUMBLE:
				clauseInterval = String.join(",", ROW_TIME_COL_NAME,
					parseWindowTime(getParams().get(TumbleTimeWindowParams.WINDOW_TIME)));
				break;
			case HOP:
				clauseInterval = String.join(",", ROW_TIME_COL_NAME,
					parseWindowTime(getParams().get(HopTimeWindowParams.HOP_TIME)),
					parseWindowTime(getParams().get(HopTimeWindowParams.WINDOW_TIME))
				);
				break;
			case SESSION:
				clauseInterval = String.join(",", ROW_TIME_COL_NAME,
					parseWindowTime(getParams().get(SessionTimeWindowParams.SESSION_GAP_TIME))
				);
				break;
			default:
				throw new IllegalArgumentException("Not support this type : " + this.windowType);
		}

		ArrayList <String> partitionCols = new ArrayList <>();
		String[] groupCols = getGroupCols();
		if (groupCols != null) {
			for (String s : groupCols) {
				if (null != s && s.trim().length() > 0 && !partitionCols.contains(s.trim())) {
					partitionCols.add(s.trim());
				}
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

	public static String parseWindowTime(String windowTime) {
		return parseWindowTime(parseTime(windowTime));
	}

	public static String parseWindowTime(Tuple2 <Double, String> tuple2) {
		return parseWindowTime(tuple2.f0, tuple2.f1);
	}

	public static String parseWindowTime(Double windowTime, String windowWithUnit) {
		if (windowTime != null) {
			return String.format("INTERVAL %s", DateUtil.parseSecondTime(windowTime));
		} else if (windowWithUnit != null) {
			String unit = windowWithUnit.substring(windowWithUnit.length() - 1);
			int ti = Integer.parseInt(windowWithUnit.substring(0, windowWithUnit.length() - 1));
			switch (unit) {
				case "s":
					return String.format("INTERVAL '%s' SECOND", ti);
				case "m":
					return String.format("INTERVAL '%s' MINUTE", ti);
				case "h":
					return String.format("INTERVAL '%s' HOUR", ti);
				case "d":
					return String.format("INTERVAL '%s' DAY", ti);
				case "M":
					return String.format("INTERVAL '%s' MONTH", ti);
				case "y":
					return String.format("INTERVAL '%s' YEAR", ti);
				default:
					throw new RuntimeException("Is is not support unit." + unit);
			}
		} else {
			throw new RuntimeException("windowTime or windowWithUnit must be set, and only one can be set.");
		}

	}

	public static Tuple2 <Double, String> parseTime(String timeIntervalStr) {
		String timeIntervalWithUnit = timeIntervalStr;
		Double timeInterval = null;
		try {
			timeInterval = Double.parseDouble(timeIntervalWithUnit);
		} catch (Exception ex) {

		}

		if (timeInterval != null) {
			timeIntervalWithUnit = null;
		}

		return Tuple2.of(timeInterval, timeIntervalWithUnit);
	}

}
