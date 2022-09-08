package com.alibaba.alink.operator.common.sql;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.TableSourceLocalOp;

/**
 * Execute SQL on MTables with local Calcite engine, which is similar to {@link BatchSqlOperators}.
 */
public class LocalOpCalciteSqlExecutor implements SqlExecutor <LocalOperator <?>> {

	private final MTableCalciteSqlExecutor mTableCalciteSqlExecutor;

	public LocalOpCalciteSqlExecutor(LocalMLEnvironment env) {
		mTableCalciteSqlExecutor = new MTableCalciteSqlExecutor(env);
	}

	@Override
	public void addTable(String name, LocalOperator <?> localOperator) {
		mTableCalciteSqlExecutor.addTable(name, localOperator.getOutputTable());
	}

	@Override
	public void removeTable(String name) {
		mTableCalciteSqlExecutor.removeTable(name);
	}

	@Override
	public void addFunction(String name, ScalarFunction function) {
		mTableCalciteSqlExecutor.addFunction(name, function);
	}

	@Override
	public void addFunction(String name, TableFunction <Row> function) {
		mTableCalciteSqlExecutor.addFunction(name, function);
	}

	@Override
	public LocalOperator <?> query(String sql) {
		return new TableSourceLocalOp(mTableCalciteSqlExecutor.query(sql));
	}

	@Override
	public LocalOperator <?> as(LocalOperator <?> localOperator, String fields) {
		return new TableSourceLocalOp(mTableCalciteSqlExecutor.as(localOperator.getOutputTable(), fields));
	}
}
