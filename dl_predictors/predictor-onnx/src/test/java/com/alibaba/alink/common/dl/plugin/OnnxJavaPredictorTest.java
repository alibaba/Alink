package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper.PredictorConfig;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;

public class OnnxJavaPredictorTest {
	@Test
	public void test() throws IOException {
		File file = File.createTempFile("cnn_mnist_pytorch", ".onnx");
		InputStream modelInputStream = getClass().getResourceAsStream("/cnn_mnist_pytorch.onnx");
		assert modelInputStream != null;
		Files.copy(modelInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);

		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		OnnxJavaPredictor predictor = new OnnxJavaPredictor();
		PredictorConfig config = new PredictorConfig();
		config.modelPath = file.getAbsolutePath();
		config.inputNames = new String[] {"0"};
		config.outputNames = new String[] {"21"};
		config.outputTypeClasses = new Class <?>[] {FloatTensor.class};
		config.threadMode = false;
		predictor.open(config);

		FloatTensor input = new FloatTensor(new Shape(1, 1, 28, 28));
		List <?> outputs = predictor.predict(Collections.singletonList(input));
		predictor.close();
		System.out.println(outputs.get(0));
		//noinspection ResultOfMethodCallIgnored
		file.delete();
	}
}
