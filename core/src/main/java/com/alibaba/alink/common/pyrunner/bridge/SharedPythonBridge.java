package com.alibaba.alink.common.pyrunner.bridge;

/**
 * All callers of `SharedPythonBridge` share the same Py4J GatewayServer and CallbackClient, and also the Python process.
 * <p>
 * Due to GIL (Global Interpreter Lock), callbacks in Python side are not processed in parallel.
 */
public class SharedPythonBridge extends BasePythonBridge {

	private static volatile BasePythonBridge inst = null;

	private SharedPythonBridge() {
		super();
	}

	public static BasePythonBridge inst() {
		if (inst == null) {
			synchronized (BasePythonBridge.class) {
				if (inst == null) {
					inst = new SharedPythonBridge();
				}
			}
		}
		return inst;
	}
}
