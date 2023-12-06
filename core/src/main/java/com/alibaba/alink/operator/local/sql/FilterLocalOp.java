package com.alibaba.alink.operator.local.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.FilterParams;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Filter records in the batch operator.
 */
@NameCn("SQL操作：Filter")
public final class FilterLocalOp extends BaseSqlApiLocalOp <FilterLocalOp>
	implements FilterParams <FilterLocalOp> {

	private static final long serialVersionUID = 1182682104232353734L;

	public FilterLocalOp() {
		this(new Params());
	}

	public FilterLocalOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public FilterLocalOp(Params params) {
		super(params);
	}

	@Override
	public FilterLocalOp linkFrom(LocalOperator <?>... inputs) {
		String predicate = getClause();
		LocalOperator <?> in = inputs[0];

		if (predicate.contains(">=")) {
			String[] splits = predicate.split(">=");
			if (splits.length == 2) {
				String colName = splits[0].trim();
				double comVal = Double.parseDouble(splits[1].trim());
				if (colName.contains("`")) {
					colName = colName.replace("`", "");
				}

				int colIdx = TableUtil.findColIndex(in.getSchema(), colName);
				if (colIdx >= 0) {
					List <Row> rows = in.collect();
					List <Row> outRows = new ArrayList <>();
					for (Row row : rows) {
						Object val = row.getField(colIdx);
						if (val != null && ((Number) val).doubleValue() >= comVal) {
							outRows.add(row);
						}
					}
					this.setOutputTable(new MTable(outRows, in.getSchema()));
					return this;
				}
			}
		} else if (predicate.contains("=")) {
			String[] splits = predicate.split("=");
			if (splits.length == 2) {
				String colName = splits[0].trim();
				String comVal = splits[1];
				if (colName.contains("`")) {
					colName = colName.replace("`", "");
				}
				if (TableUtil.isString(TableUtil.findColType(in.getSchema(), colName))) {
					comVal = comVal.replace("'", "");
				}

				int colIdx = TableUtil.findColIndex(inputs[0].getSchema(), colName);
				if (colIdx >= 0) {
					List <Row> rows = inputs[0].collect();
					List <Row> outRows = new ArrayList <>();
					for (Row row : rows) {
						Object val = row.getField(colIdx);
						if (val != null && String.valueOf(val).equals(comVal)) {
							outRows.add(row);
						}
					}
					this.setOutputTable(new MTable(outRows, inputs[0].getSchema()));
					return this;
				}
			}
		}

		LocalOperator <?> outOp;
		try {
			outOp = LocalMLEnvironment.getInstance().getSqlExecutor().filter(this, predicate);
		} catch (Exception ex) {
			// get select clause
			String[] colNames = in.getColNames();
			String[] newColNames = colNames.clone();
			for (int i = 0; i < newColNames.length; i++) {
				newColNames[i] = "`" + newColNames[i] + "`";
			}
			String selectClause = String.join(",", newColNames);
			String filterSqlClause = predicate;
			TypeInformation <?>[] types = in.getColTypes();

			// when col name not in bracket(), name replace with nameTs.
			//String[] newColNames = colNames;
			for (int i = 0; i < types.length; i++) {
				String name = colNames[i];
				String nameTs = name + "__ak_ts__";
				if (Types.SQL_TIMESTAMP == types[i] && filterSqlClause.contains(name)) {
					//newColNames[i] = nameTs + " as " + name;
					int idx = filterSqlClause.indexOf(name);
					while (idx >= 0) {
						int rightBracket = filterSqlClause.indexOf(")", idx);
						int leftBracket = filterSqlClause.indexOf("(", idx);
						if (rightBracket == -1 || leftBracket == -1 || leftBracket < rightBracket) {
							filterSqlClause = filterSqlClause.substring(0, idx)
								+ nameTs +
								filterSqlClause.substring(idx + name.length());
							idx += nameTs.length();
						} else {
							idx += name.length();
						}
						idx = filterSqlClause.indexOf(name, idx);
					}
				}
			}

			System.out.println("filter: " + filterSqlClause);
			outOp = LocalMLEnvironment.getInstance().getSqlExecutor().filter(in, selectClause, filterSqlClause);
		}
		this.setOutputTable(outOp.getOutputTable());
		return this;
	}
}
