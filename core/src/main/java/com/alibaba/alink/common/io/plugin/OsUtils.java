package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;

import static com.alibaba.alink.common.io.plugin.OsType.LINUX;

public class OsUtils {
	public static OsType getSystemType() {
		String p = System.getProperty("os.name").toLowerCase();
		if (p.contains("linux")) {
			return LINUX;
		} else if (p.contains("os x") || p.contains("darwin")) {
			return OsType.MACOSX;
		} else if (p.contains("windows")) {
			return OsType.WINDOWS;
		} else {
			throw new AkUnsupportedOperationException("Unsupported operating system: " + p);
		}
	}
}
