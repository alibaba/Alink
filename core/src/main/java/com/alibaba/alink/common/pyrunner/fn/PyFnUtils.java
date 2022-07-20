package com.alibaba.alink.common.pyrunner.fn;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.FileSystemDownloadUtils;
import com.alibaba.alink.common.utils.JsonConverter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class PyFnUtils {

	private static final String KEY_PATHS = "paths";
	private static final String KEY_FILE_PATHS = "filePaths";

	/**
	 * Handle files download in `fnSpec`.
	 * <p>
	 * If `fnSpec` has `paths`, copy files represented by `paths` to `workDir` and replace `paths`.
	 * <p>
	 * If `fnSpec` doesn't have `paths`, but has `filePaths`, download  files represented by `filePaths` to `workDir`
	 * and replace `paths`.
	 *
	 * @param fnSpec  Fn specification.
	 * @param workDir working directory.
	 * @return updated Fn specification.
	 */
	public static JsonObject downloadFilePaths(JsonObject fnSpec, Path workDir) {
		if (fnSpec.has(KEY_PATHS)) {
			JsonArray pathsJsonArray = fnSpec.get(KEY_PATHS).getAsJsonArray();
			fnSpec.remove(KEY_PATHS);
			JsonArray updatedPathsJsonArray = new JsonArray();
			for (JsonElement jsonElement : pathsJsonArray) {
				String path = jsonElement.getAsString();
				Path src = Paths.get(path);
				String fileName = src.toFile().getName();
				Path dst = workDir.resolve(fileName);
				try {
					Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
				} catch (IOException e) {
					throw new AkUnclassifiedErrorException(String.format("Failed to copy from %s to %s.",
						src.toAbsolutePath(), dst.toAbsolutePath()), e);
				}
				updatedPathsJsonArray.add(new JsonPrimitive(dst.toAbsolutePath().toString()));
			}
			fnSpec.add(KEY_PATHS, updatedPathsJsonArray);
		} else if (fnSpec.has(KEY_FILE_PATHS)) {
			JsonArray filePathsJsonArray = fnSpec.get(KEY_FILE_PATHS).getAsJsonArray();
			fnSpec.remove(KEY_FILE_PATHS);
			JsonArray pathsJsonArray = new JsonArray();
			for (JsonElement jsonElement : filePathsJsonArray) {
				FilePath filePath = FilePath.deserialize(jsonElement.getAsString());
				String filename = filePath.getPath().getName();
				Path target = workDir.resolve(filename);
				try {
					FileSystemDownloadUtils.download(filePath, target);
				} catch (IOException e) {
					throw new AkUnclassifiedErrorException(
						String.format("Failed to download %s to %s", filePath.serialize(), target), e);
				}
				pathsJsonArray.add(new JsonPrimitive(target.toAbsolutePath().toString()));
			}
			fnSpec.add(KEY_PATHS, pathsJsonArray);
		}
		return fnSpec;
	}

	public static String downloadFilePaths(String fnSpecJson, Path workDir) {
		//noinspection deprecation
		JsonObject fnSpec = JsonConverter.gson.fromJson(fnSpecJson, JsonObject.class);
		return downloadFilePaths(fnSpec, workDir).toString();
	}
}
