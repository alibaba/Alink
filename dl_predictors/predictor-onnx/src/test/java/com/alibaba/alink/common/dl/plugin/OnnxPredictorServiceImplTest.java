package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper.PredictorConfig;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
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
	private final FloatTensor input;
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
		configThreadMode.inputNames = new String[] {"0"};
		configThreadMode.outputNames = new String[] {"21"};
		configThreadMode.intraOpNumThreads = 4;
		configThreadMode.outputTypeClasses = new Class <?>[] {FloatTensor.class};
		configThreadMode.threadMode = true;

		PredictorConfig configProcessMode = new PredictorConfig();
		configProcessMode.modelPath = file.getAbsolutePath();
		configProcessMode.inputNames = new String[] {"0"};
		configProcessMode.outputNames = new String[] {"21"};
		configProcessMode.intraOpNumThreads = 4;
		configProcessMode.outputTypeClasses = new Class <?>[] {FloatTensor.class};
		configProcessMode.threadMode = false;

		FloatTensor legalInput = new FloatTensor(new Shape(1, 1, 28, 28));
		FloatTensor illegalInput = new FloatTensor(new Shape(1, 28, 28));

		return Arrays.asList(
			new Object[] {configThreadMode, legalInput, null, null},
			new Object[] {configThreadMode, illegalInput,
				RuntimeException.class, "Exception thrown in inference process."},
			new Object[] {configProcessMode, legalInput, null, null},
			new Object[] {configProcessMode, illegalInput,
				RuntimeException.class, "Exception thrown in inference process."}
		);
	}

	public OnnxPredictorServiceImplTest(PredictorConfig config, FloatTensor input,
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
		predictor.open(config.toMap());

		if (null != expectedException) {
			thrown.expect(expectedException);
			thrown.expectMessage(expectedExceptionMessage);
		}

		List <?> outputs = predictor.predict(Collections.singletonList(input));
		predictor.close();
		System.out.println(outputs.get(0));
	}
}
