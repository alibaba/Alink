package com.alibaba.alink.operator.common.feature.featurebuilder;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.sql.builtin.BuildInAggRegister;
import com.alibaba.alink.common.sql.builtin.UdafName;
import com.alibaba.alink.common.sql.builtin.agg.MTableAgg;
import com.alibaba.alink.common.utils.TableUtil;
import org.apache.commons.lang3.EnumUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class FeatureClauseUtil {

	private static final String ERROR_MESSAGE = "expressions must op(incol) as outcol, eg, sum(f1) as outf1.";

	public static FeatureClause[] extractFeatureClauses(String exprStr) {
		if (exprStr == null || exprStr.isEmpty()) {
			throw new AkIllegalOperatorParameterException("expressions must be set");
		}
		String[] clauses = splitClause(exprStr);
		FeatureClause[] featureClauses = new FeatureClause[clauses.length];
		for (int i = 0; i < clauses.length; i++) {
			String[] opAndResCol = trimArray(clauses[i].split(" (?i)as "));
			if (opAndResCol.length != 2) {
				throw new AkIllegalOperatorParameterException(ERROR_MESSAGE);
			}
			FeatureClause featureClause = new FeatureClause();
			featureClause.outColName = opAndResCol[1].trim();
			String[] opAndInput = opAndResCol[0].split("\\(");
			if (opAndInput.length != 2) {
				throw new AkIllegalOperatorParameterException(ERROR_MESSAGE);
			}
			featureClause.op = FeatureClauseOperator
				.valueOf(opAndInput[0].trim().toUpperCase());
			String[] inputContent = opAndInput[1].split("\\)");
			if (inputContent.length != 0) {
				if (inputContent.length != 1) {
					throw new AkIllegalOperatorParameterException(ERROR_MESSAGE);
				}

				if (inputContent[0].contains(",")) {
					String[] inputColAndParams = splitInputParam(inputContent[0]);
					featureClause.inColName = inputColAndParams[0].trim();
					featureClause.inputParams = new Object[inputColAndParams.length - 1];
					for (int j = 1; j < inputColAndParams.length; j++) {
						String temp = inputColAndParams[j].trim();
						int tempSize = temp.length();
						if (temp.charAt(0) == "\"" .charAt(0) && temp.charAt(tempSize - 1) == "\"" .charAt(0) ||
							temp.charAt(0) == "\'" .charAt(0) && temp.charAt(tempSize - 1) == "\'" .charAt(0)) {
							featureClause.inputParams[j - 1] = inputColAndParams[j].trim().substring(1, tempSize - 1);
						} else {
							featureClause.inputParams[j - 1] = inputColAndParams[j].trim();
						}
					}
				} else {
					featureClause.inColName = inputContent[0].trim();
					if (featureClause.op.equals(FeatureClauseOperator.LAST_VALUE) ||
						featureClause.op.equals(FeatureClauseOperator.LAST_TIME) ||
						featureClause.op.equals(FeatureClauseOperator.SUM_LAST)) {
						featureClause.inputParams = new Object[] {1};
					}
				}
			}
			featureClauses[i] = featureClause;
		}
		return featureClauses;
	}

	private static String[] splitClause(String exprStr) {
		String[] clauses = trimArray(extractClause(exprStr));
		List <String> strList = new ArrayList <>();
		int i = 0;
		while (i < clauses.length) {
			if (clauses[i].contains("(") && clauses[i].contains(")")) {
				strList.add(clauses[i]);
				i = i + 1;
			} else {
				strList.add(clauses[i] + "," + clauses[i + 1]);
				i = i + 2;
			}
		}
		return trimArray(strList.toArray(new String[0]));
	}

	private static String[] trimArray(String[] inStr) {
		String[] outStr = new String[inStr.length];
		for (int i = 0; i < inStr.length; i++) {
			outStr[i] = inStr[i].trim();
		}
		return outStr;
	}

	public static HashSet <String> aggHideTimeCol = new HashSet <>();

	public static HashSet <String> groupWindowTimeCol = new HashSet <>();

	static {
		aggHideTimeCol.add(UdafName.LAST_DISTINCT.name);
		aggHideTimeCol.add(UdafName.LAST_DISTINCT.name + BuildInAggRegister.CONSIDER_NULL_EXTEND);
		aggHideTimeCol.add(UdafName.LAST_VALUE.name);
		aggHideTimeCol.add(UdafName.LAST_VALUE.name + BuildInAggRegister.CONSIDER_NULL_EXTEND);
		aggHideTimeCol.add(UdafName.SUM_LAST.name);

		groupWindowTimeCol.add("TUMBLE_START");
		groupWindowTimeCol.add("TUMBLE_END");
		groupWindowTimeCol.add("TUMBLE_ROWTIME");
		groupWindowTimeCol.add("HOP_START");
		groupWindowTimeCol.add("HOP_END");
		groupWindowTimeCol.add("HOP_ROWTIME");
		groupWindowTimeCol.add("HOP_PROCTIME");
		groupWindowTimeCol.add("SESSION_START");
		groupWindowTimeCol.add("SESSION_END");
		groupWindowTimeCol.add("SESSION_ROWTIME");
		groupWindowTimeCol.add("SESSION_PROCTIME");
	}

	public static boolean containsAggNeedTimeColAndTimeInterval(String clause) {
		String lowerClause = clause.toLowerCase(Locale.ROOT);
		for (String s : aggHideTimeCol) {
			if (lowerClause.contains(s)) {
				return true;
			}
		}
		return false;
	}

	public static boolean isRankAggFunc(String clause) {
		String lowerClause = clause.toLowerCase(Locale.ROOT);
		return lowerClause.contains(UdafName.RANK.name) ||
			lowerClause.contains(UdafName.DENSE_RANK.name) ||
			lowerClause.contains(UdafName.ROW_NUMBER.name);
	}

	public static boolean isLastTime(String clause) {
		String lowerClause = clause.toLowerCase(Locale.ROOT);
		return lowerClause.contains(UdafName.LAST_TIME.name);
	}

	public static boolean isGroupWindowTimeCol(String operaFunc) {
		return groupWindowTimeCol.contains(operaFunc);
	}

	public static class ClauseInfo {
		int clauseNum;
		public FeatureClauseOperator[] operators;
		public int operatorIndex;
		public String[] inputCols;//this is help to identify the output type.
		public String[] asCols;
		List <String> timeCols;
		private static final String DISTINCT_CLAUSE = "distinct ";
		private static final String ALL_CLAUSE = "all ";

		public ClauseInfo(int clauseNum) {
			this.clauseNum = clauseNum;
			this.operators = new FeatureClauseOperator[clauseNum];
			this.operatorIndex = 0;
			this.inputCols = new String[clauseNum];
			this.asCols = new String[clauseNum];
			this.timeCols = new ArrayList <>();
		}

		public void addClauseInfo(String operator, String asCol) {
			String[] funcAndName = operator.split("\\(");
			String funcName = funcAndName[0].trim().toUpperCase();
			if (EnumUtils.isValidEnum(FeatureClauseOperator.class, funcName)) {
				this.operators[operatorIndex] = FeatureClauseOperator.valueOf(funcName);
			} else {
				this.operators[operatorIndex] = null;
				if (isGroupWindowTimeCol(funcName)) {
					timeCols.add(asCol);
				}
			}

			if (funcAndName.length == 2) {
				String funcParams;
				//for no param input.
				if (funcAndName[1].trim().equals(")")) {
					funcParams = "";
				} else {
					//parse distinct and all keyword.
					funcParams = funcAndName[1].split("\\)")[0].trim().toLowerCase();
					if (funcParams.contains(DISTINCT_CLAUSE)) {
						funcParams = funcParams.split(DISTINCT_CLAUSE)[1].trim();
					} else if (funcParams.contains(ALL_CLAUSE)) {
						funcParams = funcParams.split(ALL_CLAUSE)[1].trim();
					}
				}
				if (funcParams.contains(",")) {
					if (this.operators[operatorIndex] == FeatureClauseOperator.LAST_DISTINCT ||
						this.operators[operatorIndex] == FeatureClauseOperator.LAST_DISTINCT_INCLUDING_NULL) {
						funcParams = funcParams.split(",")[1].trim();
					} else {
						funcParams = funcParams.split(",")[0].trim();
					}
				}
				this.inputCols[operatorIndex] = funcParams;
			} else {
				this.inputCols[operatorIndex] = funcAndName[0];
			}
			this.asCols[operatorIndex++] = asCol;
		}

		public String[] getGroupWindowTimeCol() {
			return timeCols.toArray(new String[0]);
		}

	}

	public static String[][] splitClauseForMultiInput(String exprStr) {
		String[] splitByAs = exprStr.split(",");
		List <String[]> res = new ArrayList <>();
		int splitSize = splitByAs.length;
		int i = 0;

		while (i < splitSize) {
			String[] splitClause = splitByAs[i].split(" (?i)as ");
			if (splitClause.length == 2) {
				res.add(splitClause);
			} else {
				if (splitClause[0].contains("(")) {
					StringBuilder sbd = new StringBuilder();
					sbd.append(splitClause[0]).append(",");
					++i;
					while (!splitByAs[i].contains(")")) {
						sbd.append(splitByAs[i]).append(",");
						++i;
					}
					splitClause = splitByAs[i].split(" (?i)as ");
					sbd.append(splitClause[0]);
					res.add(new String[] {sbd.toString(), splitClause[1]});
				} else {
					res.add(new String[] {splitClause[0], splitClause[0]});
				}
			}
			++i;
		}
		return res.toArray(new String[0][0]);
	}

	private static String[] splitInputParam(String exprStr) {
		String[] result = exprStr.split(",");
		List <String> res = new ArrayList <>();
		int infoSize = result.length;
		int index = 0;
		while (index < infoSize) {
			if (result[index].contains("'") || result[index].contains("\"") &&
				index + 1 < infoSize && (
				isSplitted(result[index], result[index + 1], "'") ||
					isSplitted(result[index], result[index + 1], "\""))) {
				res.add(result[index] + "," + result[index + 1]);
				++index;
			} else {
				res.add(result[index]);
			}
			++index;
		}
		return res.toArray(new String[0]);
	}

	private static boolean isSplitted(String str1, String str2, String ele) {
		return str1.contains(ele) && (str1.split(ele).length) - 1 == 1 &&
			str2.contains(ele) && (str2.split(ele).length) - 1 == 1;
	}

	public static void buildOperatorClause(String[] operatorFunc, String[] operators, int clauseIndex,
										   String operator, String timeCol, double timeInterval,
										   TableSchema tableSchema, MLEnvironment env) {
		operatorFunc[clauseIndex] = operator.split("\\(")[0].trim().toUpperCase();
		if (isLastTime(operatorFunc[clauseIndex])) {
			String[] components = operator.split("\\)");
			if (components[0].trim().endsWith("(")) {
				operators[clauseIndex] = components[0] + timeCol + ", " + timeInterval + ")";
			} else {
				operators[clauseIndex] = components[0] + ", " + timeCol + ", " + timeInterval + ")";
			}
		} else if (containsAggNeedTimeColAndTimeInterval(operatorFunc[clauseIndex])) {
			String[] components = operator.split("\\)");
			operators[clauseIndex] = components[0] + ", " + timeCol + ", " + timeInterval + ")";
		} else if (isRankAggFunc(operatorFunc[clauseIndex])) {
			operators[clauseIndex] = operatorFunc[clauseIndex] + "(unix_timestamp(" + timeCol + "))";
		} else if (operatorFunc[clauseIndex].startsWith("MTABLE_AGG")) {
			operators[clauseIndex] = registMTableAgg(operator, operatorFunc[clauseIndex], env, tableSchema, timeCol);
		} else if (operatorFunc[clauseIndex].equals("LAST_VALUE")) {
			String[] components = operator.split("\\(");
			String[] components2 = components[1].split("\\)");
			operators[clauseIndex] = UdafName.LAST_VALUE.name + "(" +
				components2[0] + ", " + timeCol + ", " + timeInterval + ")";
		} else {
			operators[clauseIndex] = operator;
		}
	}

	public static String registMTableAgg(String clause, String operatorFunc,
										 MLEnvironment mlEnv, TableSchema tableSchema, String timeCol) {
		String aggName = "mtable_agg_" + UUID.randomUUID().toString().replace("-", "");

		if ("MTABLE_AGG" .equals(operatorFunc)) {
			mlEnv.getStreamTableEnvironment().registerFunction(aggName,
				new MTableAgg(false, getMTableSchema(clause, tableSchema), timeCol));
		} else {
			mlEnv.getStreamTableEnvironment().registerFunction(aggName,
				new MTableAgg(true, getMTableSchema(clause, tableSchema), timeCol));
		}

		return aggName + "(" + clause.split("\\(")[1];
	}

	//mtable_agg(f1, f2, f3)
	public static String getMTableSchema(String clause, TableSchema tableSchema) {
		String[] colNames = clause.split("\\(")[1].split("\\)")[0].split(",");
		Arrays.setAll(colNames, i -> colNames[i].trim());
		TypeInformation[] newTypes = TableUtil.findColTypes(tableSchema, colNames);
		return TableUtil.schema2SchemaStr(new TableSchema(colNames, newTypes));
	}

	//considering "," may be used in operator param. When splitting, we have to consider this situation.
	private static String[] extractClause(String clauseStr) {
		String[] splittedClauses = clauseStr.split(",");
		List <String> res = new ArrayList <>();
		int lens = splittedClauses.length;
		int index = 0;
		while (index < lens) {
			if (splittedClauses[index].split(" (?i)as ").length == 2) {
				res.add(splittedClauses[index]);
			} else {
				if (index + 1 == lens) {
					throw new AkIllegalOperatorParameterException("");
				}
				res.add(splittedClauses[index] + "," + splittedClauses[++index]);
			}
			++index;
		}
		return res.toArray(new String[0]);
	}
}
