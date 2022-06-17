package com.alibaba.alink.common.viz.plot;

import com.alibaba.alink.common.utils.AlinkSerializable;

public class Histogram implements AlinkSerializable {
	public String name;
	public Interval[] intervals;
	public double step;
}

