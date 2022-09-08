package com.alibaba.alink.common.fe.define.statistics;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.io.Serializable;

public interface BaseStatistics extends Serializable, AlinkSerializable {
	String name();
}
