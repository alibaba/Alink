package com.alibaba.alink.common.io.plugin;

import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

public abstract class ClassLoaderFactory implements Serializable {

	private static final long serialVersionUID = -7228535414098842970L;

	protected final RegisterKey registerKey;
	protected final DistributeCache distributeCache;

	protected final static Object EMPTY_RETURN = null;

	public ClassLoaderFactory(RegisterKey registerKey,
							  DistributeCache distributeCache) {
		this.registerKey = registerKey;
		this.distributeCache = distributeCache;
	}

	public void doAs(PrivilegedExceptionActionWithoutReturn action) throws Exception {
		doAs(() -> {
			action.run();
			return EMPTY_RETURN;
		});
	}

	public void doAsThrowRuntime(PrivilegedExceptionActionWithoutReturn action) {
		try {
			doAs(action);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public <T> T doAsThrowRuntime(PrivilegedExceptionAction <T> action) {
		try {
			return doAs(action);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public <T> T doAs(PrivilegedExceptionAction <T> action) throws Exception {

		ClassLoader classLoader = create();

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			return action.run();
		}
	}

	public abstract ClassLoader create();
}
