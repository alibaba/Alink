package com.alibaba.alink.common.dl;

import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Store paths that need to be downloaded, and the name mapping if some files need to be renamed.
 * <p>
 * For compressed files, by default, they will be decompressed to the downloading directory directly. If name mapping
 * specified, they will be decompressed to a subdirectory with the specified name.
 * <p>
 * The actual downloading and renaming procedure is done in {@link com.alibaba.alink.common.dl.utils.DataSetDiskDownloader#downloadFilesWithRename}.
 * <p>
 * Local files (whose paths started with "file://") should not be added to an instance of {@link ExternalFilesConfig}.
 * The reason is that symbolic links are used to avoid unnecessary copy, but they cannot be created without admin
 * permission on Windows. Right now, the algorithms themselves should take care of such paths.
 */
public class ExternalFilesConfig implements Serializable {
	Set <String> filePaths = new HashSet <>();
	Map <String, String> fileRenameMap = new HashMap <>();

	public static ExternalFilesConfig fromJson(String s) {
		return JsonConverter.fromJson(s, ExternalFilesConfig.class);
	}

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
}
