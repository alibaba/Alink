package com.alibaba.alink.operator.common.sql;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.utils.TableUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectUtils {
	/*
	  convert regex in select clause, eg: colNames: {"pt","pt_id"} clause:"(pt)?+.+" return: "pt_id"
	 */
	static public String convertRegexClause2ColNames(String[] colNames, String clause) {
		StringBuilder newClauseBuilder = new StringBuilder();
		StringBuilder rePatternBuilder = new StringBuilder();
		StringBuilder stringBuilderPointer = newClauseBuilder;
		List <String> rePatterns = new ArrayList <>();
		boolean appendComma = true;
		boolean isResultCol = false;
		String isResultColPattern = "(?i)\\sas(\\s+)$";
		String extraCommaPattern = ",(\\s*)$";
		Pattern p = Pattern.compile(isResultColPattern);
		Matcher m;
		for (int i = 0; i < clause.length(); i++) {
			char currentChar = clause.charAt(i);
			if ('`' == (currentChar)) {
				if (rePatternBuilder.length() == 0 && !isResultCol) {
					m = p.matcher(newClauseBuilder.toString());
					if (m.find()) {
						isResultCol = true;
						stringBuilderPointer.append(currentChar);
						continue;
					}
					rePatternBuilder.append(currentChar);
					stringBuilderPointer = rePatternBuilder;
				} else if (isResultCol) {
					isResultCol = false;
					stringBuilderPointer.append(currentChar);
				} else {
					String matchColumns = getMatchColumns(colNames, rePatternBuilder.substring(1));
					if (matchColumns.length() > 0) {
						newClauseBuilder.append(matchColumns);
					} else {
						appendComma = false;
					}
					stringBuilderPointer = newClauseBuilder;
					rePatternBuilder.setLength(0);
				}
			} else {
				if (currentChar == ',' && !appendComma) {
					appendComma = true;
				} else {
					stringBuilderPointer.append(currentChar);
				}
			}
		}
		String s = newClauseBuilder.toString();
		// remove  ',' on the start/end position and continue ','
		p = Pattern.compile(extraCommaPattern);
		m = p.matcher(s);

		if (m.find()) {
			s = s.substring(0, m.start());
		}

		return s;
	}

	static private String getMatchColumns(String[] colNames, String regex) {
		StringBuilder matchColumns = new StringBuilder();
		for (String colName : colNames) {
			if (colName.matches(regex)) {
				matchColumns.append(",`").append(colName).append("`");
			}
		}
		if (matchColumns.length() > 0) {
			return matchColumns.substring(1);
		} else {
			return "";
		}
	}

	/**
	 * here is simple:
	 * "col1, `col2`"
	 * "*"
	 * "*, col1 as col1_rename"
	 * "`col1`, col2 as col1"
	 * ... ...
	 */
	static public boolean isSimpleSelect(String clause, String[] colNames) {
		// col is proctime or rowtime, is not simple.
		if (clause.contains("_row_time_col_name")) {
			return false;
		}
		String newClause = replaceStar(clause, colNames);
		String[] splits = StringUtils.split(newClause, ",");

		String[] colNames2 = new String[colNames.length];
		for (int i = 0; i < colNames2.length; i++) {
			colNames2[i] = "`" + colNames[i] + "`";
		}
		for (String s : splits) {
			String[] ss = s.split(" (?i)as ");
			if (ss.length > 1) {
				if (TableUtil.findColIndex(colNames, ss[0].trim()) == -1 &&
					TableUtil.findColIndex(colNames2, ss[0].trim()) == -1) {
					return false;
				}
			} else {
				if (TableUtil.findColIndex(colNames, s.trim()) == -1 &&
					TableUtil.findColIndex(colNames2, s.trim()) == -1) {
					return false;
				}
			}
		}
		return true;
	}

	//<inputCols, outputCols>
	static public Tuple2 <String[], String[]> splitAndTrim(String clause, String[] colNames) {
		String newClause = replaceStar(clause, colNames);
		String[] splits = StringUtils.split(newClause, ",");
		String[] inputCols = new String[splits.length];
		String[] outputCols = new String[splits.length];

		for (int i = 0; i < splits.length; i++) {
			String s = splits[i].trim();
			String[] ss = s.split(" (?i)as ");
			// col0 as f0
			if (ss.length > 1) {
				inputCols[i] = trimQuoCol(ss[0]);
				outputCols[i] = trimQuoCol(ss[1]);
			} else {
				inputCols[i] = trimQuoCol(s);
				outputCols[i] = inputCols[i];
			}
		}

		//check duplicate cols and rename
		for (int i = outputCols.length - 1; i >= 0; i--) {
			int count = 0;
			for (int j = 0; j < i; j++) {
				if (outputCols[i].equals(outputCols[j])) {
					count++;
				}
			}
			if (count > 0) {
				outputCols[i] = outputCols[i] + (count-1);
			}
		}
		return Tuple2.of(inputCols, outputCols);
	}

	static public String trimQuoCol(String colName) {
		String localColName = colName.trim();
		return localColName.startsWith("`") ? localColName.substring(1, localColName.length() - 1) : localColName;
	}

	static public String replaceStar(String clause, String[] colNames) {
		String[] splits = StringUtils.split(clause, ",");
		//replace *
		String newClause = clause;
		if (clause.contains("*")) {
			int idx = -1;
			for (int i = 0; i < splits.length; i++) {
				if (splits[i].trim().equals("*")) {
					idx = i;
				}
			}
			if (idx != -1) {
				splits[idx] = StringUtils.join(colNames, ",");
			}
			newClause = StringUtils.join(splits, ",");
		}
		return newClause;
	}
}
