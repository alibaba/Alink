package com.alibaba.alink.common.fe.define;

public interface InterfaceHopWindowStatFeatures extends InterfaceWindowStatFeatures {

	String[] getWindowTimes();

	String[] getHopTimes();
}
