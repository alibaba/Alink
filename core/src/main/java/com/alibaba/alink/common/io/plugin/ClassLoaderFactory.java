package com.alibaba.alink.common.io.plugin;

import org.apache.flink.util.TemporaryClassLoaderContext;

import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

public abstract class ClassLoaderFactory implements Serializable {

	private static final long serialVersionUID = -7228535414098842970L;

	protected final RegisterKey registerKey;
	protected final Map <String, String> registerContext;

	protected final static Object EMPTY_RETURN = null;

	public ClassLoaderFactory(RegisterKey registerKey,
							  Map <String, String> registerContext) {

		this.registerKey = registerKey;
		this.registerContext = registerContext;
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
