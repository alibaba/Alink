package com.alibaba.alink.operator.common.tensorflow;

import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import com.alibaba.alink.common.dl.utils.FileDownloadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

class TFSavedModelUtils {

	private static final Logger LOG = LoggerFactory.getLogger(TFSavedModelUtils.class);

	static boolean containSavedModel(File dir) {
		File[] pbFiles = dir.listFiles((dir1, name) -> name.endsWith(".pb") || name.endsWith(".pbtxt"));
		return (null != pbFiles) && (pbFiles.length > 0);
	}

	/**
	 * Download SavedModel to a local directory and return its path.
	 *
	 * @param modelPath SavedModel path, can be a compressed file of all supported types of paths, or an OSS directory
	 *                  path (see {@link FileDownloadUtils#downloadHttpOrOssFile}).
	 * @return local path of SavedModel.
	 * <p>
	 * When the path is a compressed file, it will be automatically decompressed to a temporary directory. If the
	 * extracted content is a folder, then the folder path is returned. Otherwise, the path of the temporary directory
	 * is returned.
	 */
	static String downloadSavedModel(String modelPath) {
		String localModelPath;
		File workDir = PythonFileUtils.createTempDir("temp_");
		if (PythonFileUtils.isCompressedFile(modelPath)) {
			workDir.deleteOnExit();
			ArchivesUtils.downloadDecompressToDirectory(modelPath, workDir);
			localModelPath = workDir.getAbsolutePath();
			if (!TFSavedModelUtils.containSavedModel(workDir)) {
				LOG.info("No .pb or .pbtxt files found in {}", workDir);
				localModelPath = workDir.getAbsolutePath() + File.separator
					+ PythonFileUtils.getCompressedFileName(modelPath);
			}
		} else if (modelPath.startsWith("oss://")) {
			String fn = FileDownloadUtils.downloadHttpOrOssFile(modelPath, workDir.getAbsolutePath());
			localModelPath = workDir + File.separator + fn;
		} else if (modelPath.startsWith("file://")) {
			localModelPath = modelPath;
		} else {
			throw new UnsupportedOperationException(
				"Model path must represent a compressed file or a local directory or an OSS directory.");
		}
		return localModelPath;
	}
}
