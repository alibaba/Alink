package com.alibaba.alink.common;

import com.alibaba.alink.operator.common.sql.LocalOpCalciteSqlExecutor;
import com.alibaba.alink.operator.local.lazy.LocalLazyObjectsManager;
import com.alibaba.alink.operator.local.sql.CalciteFunctionCompiler;

public class LocalMLEnvironment {
	private static final ThreadLocal<LocalMLEnvironment> threadLocalEnv = ThreadLocal.withInitial(() -> new LocalMLEnvironment());
	/**
	 * lazy load for speed.
	 */
	private LocalOpCalciteSqlExecutor sqlExecutor;

	// Compile user defined functions. We need to use its latest classloader when executing SQL.
	private final CalciteFunctionCompiler calciteFunctionCompiler;

	private final LocalLazyObjectsManager lazyObjectsManager;

	private LocalMLEnvironment() {
		calciteFunctionCompiler = new CalciteFunctionCompiler(Thread.currentThread().getContextClassLoader());
		lazyObjectsManager = new LocalLazyObjectsManager();
	}

	public static LocalMLEnvironment getInstance() {
		return threadLocalEnv.get();
	}

	public CalciteFunctionCompiler getCalciteFunctionCompiler() {
		return calciteFunctionCompiler;
	}

	public synchronized LocalOpCalciteSqlExecutor getSqlExecutor() {
		if (sqlExecutor == null) {
			sqlExecutor = new LocalOpCalciteSqlExecutor(this);
		}
		return sqlExecutor;
	}

	public LocalLazyObjectsManager getLazyObjectsManager() {
		return lazyObjectsManager;
	}
}
