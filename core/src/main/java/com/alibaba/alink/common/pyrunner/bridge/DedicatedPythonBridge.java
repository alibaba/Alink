package com.alibaba.alink.common.pyrunner.bridge;

/**
 * Every caller of `DedicatedPythonBridge` owns a dedicated Py4J GatewayServer and CallbackClient, and also a dedicated
 * Python process.
 * <p>
 * Therefore, all Python callbacks are processed in parallel in cost of more CPU and memory usage.
 */
public class DedicatedPythonBridge extends BasePythonBridge {

	private DedicatedPythonBridge() {
		super();
	}

	public static BasePythonBridge inst() {
		return new DedicatedPythonBridge();
	}
}
