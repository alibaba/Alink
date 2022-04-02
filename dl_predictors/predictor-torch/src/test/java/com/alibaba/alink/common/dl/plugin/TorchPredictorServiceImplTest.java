package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.pytorch.LibtorchUtils.getLibtorchPath;
import static com.alibaba.alink.operator.common.pytorch.TorchScriptConstants.LIBRARY_PATH_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTRA_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.MODEL_PATH_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_TYPE_CLASSES;

public class TorchPredictorServiceImplTest {
	@Test
	public void testPredict() throws IOException {
		final String LIBTORCH_VERSION = "1.8.1";
		String libraryPath = new File(getLibtorchPath(LIBTORCH_VERSION), "lib").getAbsolutePath();
		File file = File.createTempFile("pytorch", ".pt");
		InputStream modelInputStream = getClass().getResourceAsStream("/pytorch.pt");
		assert modelInputStream != null;
		Files.copy(modelInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);

		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		TorchPredictorServiceImpl predictor = new TorchPredictorServiceImpl();
		Map <String, Object> config = new HashMap <>();
		config.put(MODEL_PATH_KEY, file.getAbsolutePath());
		config.put(LIBRARY_PATH_KEY, libraryPath);
		config.put(OUTPUT_TYPE_CLASSES, new Class[] {Tensor.class});
		config.put(INTRA_OP_PARALLELISM_KEY, 4);
		predictor.open(config);

		for (int i = 0; i < 10; i += 1) {
			List <?> inputs = Arrays.asList(
				new LongTensor(new long[][] {{1, 2, 3}, {4, 5, 6}}),
				new IntTensor(3));
			List <?> outputs = predictor.predict(inputs);
			Assert.assertEquals(new LongTensor(new long[] {11, 13, 15}), outputs.get(0));
		}
		predictor.close();
		//noinspection ResultOfMethodCallIgnored
		file.delete();
	}
}
