package com.alibaba.alink.common.dl;

import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalFilesConfig implements Serializable {
	Set <String> filePaths = new HashSet <>();
	Map <String, String> fileRenameMap = new HashMap <>();

	public ExternalFilesConfig addFilePaths(List <String> paths) {
		filePaths.addAll(paths);
		return this;
	}

	public ExternalFilesConfig addFilePaths(String... paths) {
		filePaths.addAll(Arrays.asList(paths));
		return this;
	}

	public ExternalFilesConfig addRenameMap(String path, String renameTo) {
		filePaths.add(path);
		fileRenameMap.put(path, renameTo);
		return this;
	}

	public ExternalFilesConfig addRenameMaps(Map <String, String> m) {
		filePaths.addAll(m.keySet());
		fileRenameMap.putAll(m);
		return this;
	}

	public Set <String> getFilePaths() {
		return filePaths;
	}

	public Map <String, String> getFileRenameMap() {
		return fileRenameMap;
	}

	public String toJson() {
		return JsonConverter.toJson(this);
	}

	public static ExternalFilesConfig fromJson(String s) {
		return JsonConverter.fromJson(s, ExternalFilesConfig.class);
	}
}
