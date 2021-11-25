package com.alibaba.alink.common.io.plugin;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public abstract class DistributeCache implements Serializable {

	public abstract Map <String, String> context();

	public abstract void distributeAsLocalFile() throws IOException;
}
