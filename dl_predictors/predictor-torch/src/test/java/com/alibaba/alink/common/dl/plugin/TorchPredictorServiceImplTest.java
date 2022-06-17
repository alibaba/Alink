package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper.PredictorConfig;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
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

import static com.alibaba.alink.operator.common.pytorch.LibtorchUtils.getLibtorchPath;

@RunWith(Parameterized.class)
public class TorchPredictorServiceImplTest {

	private final PredictorConfig config;
	private final List <?> input;
	private final List <?> output;
	private final Class <? extends Exception> expectedException;
	private final String expectedExceptionMessage;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Parameters
	public static Collection <Object[]> data() throws IOException {
		final String LIBTORCH_VERSION = "1.8.1";
		final String libtorchPath = getLibtorchPath(new ResourcePluginFactory(), LIBTORCH_VERSION);
		String libraryPath = new File(libtorchPath, "lib").getAbsolutePath();

		File file = File.createTempFile("pytorch", ".pt");
		InputStream modelInputStream = TorchPredictorServiceImplTest.class.getResourceAsStream("/pytorch.pt");
		assert modelInputStream != null;
		Files.copy(modelInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);

		//noinspection Convert2MethodRef,ResultOfMethodCallIgnored
		Runtime.getRuntime().addShutdownHook(new Thread(() -> file.delete()));

		PredictorConfig configThreadMode = new PredictorConfig();
		configThreadMode.modelPath = file.getAbsolutePath();
		configThreadMode.factory = new TorchPredictorClassLoaderFactory("1.8.0");
		configThreadMode.libraryPath = libraryPath;
		configThreadMode.outputTypeClasses = new Class[] {Tensor.class};
		configThreadMode.intraOpNumThreads = 4;
		configThreadMode.threadMode = true;

		PredictorConfig configProcessMode = new PredictorConfig();
		configProcessMode.modelPath = file.getAbsolutePath();
		configProcessMode.factory = new TorchPredictorClassLoaderFactory("1.8.0");
		configProcessMode.libraryPath = libraryPath;
		configProcessMode.outputTypeClasses = new Class[] {Tensor.class};
		configProcessMode.intraOpNumThreads = 4;
		configProcessMode.threadMode = false;

		List <?> legalInput = Arrays.asList(
			new LongTensor(new long[][] {{1, 2, 3}, {4, 5, 6}}),
			new IntTensor(3));

		List <?> illegalInput = Arrays.asList(
			new StringTensor(new String[] {"a", "b"}),
			//new LongTensor(new long[][] {{0, 1, 2, 3}, {4, 5, 6, 7}}),
			new IntTensor(3));

		List <?> output = Collections.singletonList(new LongTensor(new long[] {11, 13, 15}));

		return Arrays.asList(
			new Object[] {configThreadMode, legalInput, output, null, null},
			new Object[] {configThreadMode, illegalInput, null,
				RuntimeException.class, "Exception thrown in inference process."},
			new Object[] {configProcessMode, legalInput, output, null, null},
			new Object[] {configProcessMode, illegalInput, null,
				RuntimeException.class, "Exception thrown in inference process."}
		);
	}

	public TorchPredictorServiceImplTest(PredictorConfig config, List <?> input, List <?> output,
										 Class <? extends Exception> expectedException,
										 String expectedExceptionMessage) {
		this.config = config;
		this.input = input;
		this.output = output;
		this.expectedException = expectedException;
		this.expectedExceptionMessage = expectedExceptionMessage;
	}

	@Test
	public void testTorchPredictorServiceImpl() {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		TorchPredictorServiceImpl predictor = new TorchPredictorServiceImpl();
		predictor.open(config);

		if (null != expectedException) {
			thrown.expect(expectedException);
			thrown.expectMessage(expectedExceptionMessage);
		}
		List <?> outputs = predictor.predict(input);
		predictor.close();
		Assert.assertEquals(outputs.size(), output.size());
		for (int i = 0; i < outputs.size(); i += 1) {
			Assert.assertEquals(outputs.get(i), output.get(i));
		}
	}
}
