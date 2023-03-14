package com.alibaba.alink.common.viz;

import com.alibaba.alink.common.viz.DummyVizManager;
import com.alibaba.alink.common.viz.VizManagerInterface;

public class ScreenManager {
	//public static Boolean ApsEnableLogging = Boolean.FALSE;
	static VizManagerInterface screenManager = new DummyVizManager();

	public static VizManagerInterface getScreenManager() {
		return screenManager;
	}

	public static void setScreenManager(VizManagerInterface manger) {
		screenManager = manger;
	}
}
