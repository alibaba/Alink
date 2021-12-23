package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import com.alibaba.alink.common.dl.utils.FileDownloadUtils;
import com.alibaba.alink.common.dl.utils.ZipFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Comparator;
import java.util.List;

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
			localModelPath = modelPath.substring("file://".length());
		} else {
			throw new UnsupportedOperationException(
				"Model path must represent a compressed file or a local directory or an OSS directory.");
		}
		return localModelPath;
	}

	public static String loadSavedModelFromZipFile(String path) {
		String workDir = PythonFileUtils.createTempWorkDir("extract_saved_model_");
		String dirName = PythonFileUtils.getCompressedFileName(path);
		File dir = new File(workDir, dirName);
		try {
			ZipFileUtil.unzipFileIntoDirectory(new File(path), dir);
		} catch (IOException e) {
			throw new RuntimeException(String.format("Failed to unzip %s to %s.", new File(path), dir), e);
		}
		return dir.getAbsolutePath();
	}

	public static String loadSavedModelFromRows(List <Row> rows) {
		String workDir = PythonFileUtils.createTempWorkDir("saved_model_zip_");
		rows = new ArrayList <>(rows);
		rows.sort(Comparator.comparingLong(d -> (Long) d.getField(0)));

		String zipFilename = (String) rows.get(0).getField(1);
		File zipFile = new File(workDir, zipFilename);
		try (FileOutputStream fos = new FileOutputStream(zipFile)) {
			Decoder decoder = Base64.getDecoder();
			for (int i = 1, modelRowsSize = rows.size(); i < modelRowsSize; i += 1) {
				Row modelRow = rows.get(i);
				fos.write(decoder.decode((String) modelRow.getField(1)));
			}
		} catch (Exception e) {
			throw new RuntimeException(String.format("Cannot extract data to %s", zipFilename), e);
		}
		return loadSavedModelFromZipFile(zipFile.getAbsolutePath());
	}
}
