package com.alibaba.alink.common;

import com.alibaba.alink.operator.common.sql.LocalOpCalciteSqlExecutor;
import com.alibaba.alink.operator.local.lazy.LocalLazyObjectsManager;
import com.alibaba.alink.operator.local.sql.CalciteFunctionCompiler;

public class LocalMLEnvironment {
	private static final LocalMLEnvironment INSTANCE = new LocalMLEnvironment();

	private final LocalOpCalciteSqlExecutor sqlExecutor;

	// Compile user defined functions. We need to use its latest classloader when executing SQL.
	private final CalciteFunctionCompiler calciteFunctionCompiler;

	private final LocalLazyObjectsManager lazyObjectsManager;

	private LocalMLEnvironment() {
		calciteFunctionCompiler = new CalciteFunctionCompiler(Thread.currentThread().getContextClassLoader());
		sqlExecutor = new LocalOpCalciteSqlExecutor(this);
		lazyObjectsManager = new LocalLazyObjectsManager();
	}

	public static LocalMLEnvironment getInstance() {
		return INSTANCE;
	}

	public CalciteFunctionCompiler getCalciteFunctionCompiler() {
		return calciteFunctionCompiler;
	}

	public LocalOpCalciteSqlExecutor getSqlExecutor() {
		return sqlExecutor;
	}

	public LocalLazyObjectsManager getLazyObjectsManager() {
		return lazyObjectsManager;
	}
}
