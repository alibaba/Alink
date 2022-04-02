package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.flink.ml.cluster.node.MLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TorchUtils {
	private static final Logger LOG = LoggerFactory.getLogger(TorchUtils.class);

	/**
	 * A hack to add libtorch path to java.library.path
	 */
	public static void addLibraryPath(String pathToAdd) throws Exception {
		Field usrPathsField = ClassLoader.class.getDeclaredField("usr_paths");
		usrPathsField.setAccessible(true);

		String[] paths = (String[]) usrPathsField.get(null);
		System.out.println(Arrays.toString(paths));

		for (String path : paths) {if (path.equals(pathToAdd)) {return;}}

		String[] newPaths = Arrays.copyOf(paths, paths.length + 1);
		newPaths[newPaths.length - 1] = pathToAdd;
		System.out.println(Arrays.toString(newPaths));
		usrPathsField.set(null, newPaths);
	}

	public static Process launchInferenceProcess(MLContext mlContext, String modelPath,
												 Class <?>[] outputTypeClasses,
												 String libraryPath, int numThreads)
		throws IOException {
		List <String> args = new ArrayList <>();
		String javaHome = System.getProperty("java.home");
		args.add(String.join(File.separator, javaHome, "bin", "java"));
		// set classpath
		List <String> cpElements = new ArrayList <>();
		// add sys classpath
		cpElements.add(System.getProperty("java.class.path"));
		// add user code classpath
		if (Thread.currentThread().getContextClassLoader() instanceof URLClassLoader) {
			for (URL url : ((URLClassLoader) Thread.currentThread().getContextClassLoader()).getURLs()) {
				cpElements.add(url.toString());
			}
		}
		args.add("-cp");
		args.add(String.join(File.pathSeparator, cpElements));

		args.add("-Djava.library.path=" + libraryPath);
		args.add(TorchJavaInferenceRunner.class.getCanonicalName());

		args.add(String.format("%s:%d", mlContext.getNodeServerIP(), mlContext.getNodeServerPort()));
		args.add(modelPath);
		args.add(JsonConverter.toJson(outputTypeClasses));

		LOG.info("Java Inference Cmd: " + String.join(" ", args));
		ProcessBuilder builder = new ProcessBuilder(args);
		builder.environment().put("OMP_NUM_THREADS", String.valueOf(numThreads));
		builder.redirectErrorStream(true);
		builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		return builder.start();
	}

}
