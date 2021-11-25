package com.alibaba.alink.common.pyrunner.bridge;

import com.alibaba.alink.common.pyrunner.PyMainHandle;
import com.alibaba.alink.testutil.categories.PyTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class PythonBridgeTest {

	@Category(PyTest.class)
	@Test
	public void testSharedPythonBridge() {
		BasePythonBridge pythonBridge = SharedPythonBridge.inst();
		pythonBridge.open("A", "python3", 0, 0, 3000, 3000, true);
		pythonBridge.open("B", "python3", 0, 0, 3000, 3000, true);
		PyMainHandle pyMain = pythonBridge.app();
		Assert.assertTrue(pyMain.check());
		pythonBridge.close("A");
		pythonBridge.close("B");
	}

	@Category(PyTest.class)
	@Test
	public void testDedicatedPythonBridge() {
		BasePythonBridge pythonBridge = DedicatedPythonBridge.inst();
		pythonBridge.open("A", "python3", 0, 0, 3000, 3000, true);
		pythonBridge.open("B", "python3", 0, 0, 3000, 3000, true);
		PyMainHandle pyMain = pythonBridge.app();
		Assert.assertTrue(pyMain.check());
		pythonBridge.close("A");
		pythonBridge.close("B");
	}
}
