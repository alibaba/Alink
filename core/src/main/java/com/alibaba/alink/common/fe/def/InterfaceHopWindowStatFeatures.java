package com.alibaba.alink.common.fe.def;

public interface InterfaceHopWindowStatFeatures extends InterfaceWindowStatFeatures {

	String[] getWindowTimes();

	String[] getHopTimes();
}
