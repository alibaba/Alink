package com.alibaba.alink.common.dl.utils;

import com.alibaba.alink.common.dl.ExternalFilesConfig;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * utils to deal with user files.
 */
public class ExternalFilesUtils {
	/**
	 * Download or copy normal file from `path` to `workDir` with possible `rename`.
	 * For protocols of "file" and "res", directory is support.
	 * @param workDir
	 * @param path
	 * @param rename
	 * @throws IOException
	 */
	static void prepareExternalNormalFile(String workDir, String path, String rename) throws IOException {
		String fn = path.substring(path.lastIndexOf('/') + 1);
		if (rename == null) {
			rename = fn;
		}
		FileDownloadUtils.downloadFile(path, new File(workDir), rename);
	}

	static void prepareExternalCompressedFile(String workDir, String path, String rename) throws IOException {
		File targetDir = rename == null
			? new File(workDir)
			: new File(workDir, rename);
		PythonFileUtils.ensureDirectoryExists(targetDir);
		ArchivesUtils.downloadDecompressToDirectory(path, targetDir);
	}

	static void prepareExternalFiles(String workDir, List <String> paths, Map <String, String> renameMap) {
		for (String path : paths) {
			String rename = renameMap.get(path);
			boolean isCompressed = PythonFileUtils.isCompressedFile(path);
			try {
				if (isCompressed) {
					prepareExternalCompressedFile(workDir, path, rename);
				} else {
					prepareExternalNormalFile(workDir, path, rename);
				}
			} catch (IOException e) {
				throw new AkUnclassifiedErrorException("Cannot prepare file: " + path, e);
			}
		}
	}

	/**
	 * Prepare all external files, including scripts, pretrained models, checkpoints, user-defined files.
	 * @param workDir
	 */
	public static void prepareExternalFiles(ExternalFilesConfig externalFiles, String workDir) {
		List <String> filePaths = new ArrayList <>(externalFiles.getFilePaths());
		Map <String, String> fileRenameMap = externalFiles.getFileRenameMap();
		prepareExternalFiles(workDir, filePaths, fileRenameMap);
	}
}
