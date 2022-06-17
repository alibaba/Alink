package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper.PredictorConfig;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class OnnxPredictorServiceImplTest {
	private final PredictorConfig config;
	private final List <?> input;
	private final Class <? extends Exception> expectedException;
	private final String expectedExceptionMessage;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Parameters
	public static Collection <Object[]> data() throws IOException {
		File file = File.createTempFile("cnn_mnist_pytorch", ".onnx");
		InputStream modelInputStream = OnnxPredictorServiceImplTest.class
			.getResourceAsStream("/cnn_mnist_pytorch.onnx");
		assert modelInputStream != null;
		Files.copy(modelInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);

		//noinspection Convert2MethodRef,ResultOfMethodCallIgnored
		Runtime.getRuntime().addShutdownHook(new Thread(() -> file.delete()));

		PredictorConfig configThreadMode = new PredictorConfig();
		configThreadMode.modelPath = file.getAbsolutePath();
		configThreadMode.factory = new OnnxPredictorClassLoaderFactory();
		configThreadMode.inputNames = new String[] {"0"};
		configThreadMode.outputNames = new String[] {"21"};
		configThreadMode.intraOpNumThreads = 4;
		configThreadMode.outputTypeClasses = new Class <?>[] {FloatTensor.class};
		configThreadMode.threadMode = true;

		PredictorConfig configProcessMode = new PredictorConfig();
		configProcessMode.modelPath = file.getAbsolutePath();
		configProcessMode.factory = new OnnxPredictorClassLoaderFactory();
		configProcessMode.inputNames = new String[] {"0"};
		configProcessMode.outputNames = new String[] {"21"};
		configProcessMode.intraOpNumThreads = 4;
		configProcessMode.outputTypeClasses = new Class <?>[] {FloatTensor.class};
		configProcessMode.threadMode = false;

		List <?> legalInput = Collections.singletonList(new FloatTensor(new Shape(1, 1, 28, 28)));
		List <?> illegalInput = Collections.singletonList(new FloatTensor(new Shape(1, 28, 28)));

		return Arrays.asList(
			new Object[] {configThreadMode, legalInput, null, null},
			new Object[] {configThreadMode, illegalInput,
				RuntimeException.class, "Exception thrown in inference process."},
			new Object[] {configProcessMode, legalInput, null, null},
			new Object[] {configProcessMode, illegalInput,
				RuntimeException.class, "Exception thrown in inference process."}
		);
	}

	public OnnxPredictorServiceImplTest(PredictorConfig config, List <?> input,
										Class <? extends Exception> expectedException,
										String expectedExceptionMessage) {
		this.config = config;
		this.input = input;
		this.expectedException = expectedException;
		this.expectedExceptionMessage = expectedExceptionMessage;
	}

	@Test
	public void testOnnxPredictorServiceImpl() {
		//AlinkGlobalConfiguration.setPrintProcessInfo(true);
		OnnxPredictorServiceImpl predictor = new OnnxPredictorServiceImpl();
		predictor.open(config);

		if (null != expectedException) {
			thrown.expect(expectedException);
			thrown.expectMessage(expectedExceptionMessage);
		}

		List <?> outputs = predictor.predict(input);
		predictor.close();
		Assert.assertTrue(outputs.get(0) instanceof FloatTensor);
		FloatTensor output = (FloatTensor) outputs.get(0);
		Assert.assertArrayEquals(new long[] {1, 10}, output.shape());
	}
}
