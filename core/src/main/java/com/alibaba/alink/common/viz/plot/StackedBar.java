package com.alibaba.alink.common.viz.plot;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StackedBar implements AlinkSerializable {
	public String[] xTag;
	public List <Map <String, Object>> data = new ArrayList <>();
}
