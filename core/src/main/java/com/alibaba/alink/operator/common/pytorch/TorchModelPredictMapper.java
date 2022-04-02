package com.alibaba.alink.operator.common.pytorch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.plugin.DLPredictorService;
import com.alibaba.alink.common.dl.plugin.TorchPredictorClassLoaderFactory;
import com.alibaba.alink.common.dl.utils.FileDownloadUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.CloseableThreadLocal;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants;
import com.alibaba.alink.params.dl.TorchModelPredictParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class TorchModelPredictMapper extends Mapper {
	private static final Logger LOG = LoggerFactory.getLogger(TorchModelPredictMapper.class);

	private final TorchPredictorClassLoaderFactory factory;

	private final static String TORCH_JAVA_VERSION = "1.8.0";
	private final static String LIBTORCH_VERSION = "1.8.1";

	private final String modelPath;
	private final int intraOpParallelism;
	private Class <?>[] outputColTypeClasses;

	private String localModelPath;
	private String libraryPath;

	private final LongAdder counter = new LongAdder();

	private transient CloseableThreadLocal <DLPredictorService> threadLocalPredictor;

	public TorchModelPredictMapper(TableSchema dataSchema, Params params) {
		this(dataSchema, params, new TorchPredictorClassLoaderFactory(TORCH_JAVA_VERSION));
	}

	public TorchModelPredictMapper(TableSchema dataSchema, Params params, TorchPredictorClassLoaderFactory factory) {
		super(dataSchema, params);
		this.factory = factory;
		this.modelPath = params.get(TorchModelPredictParams.MODEL_PATH);
		this.intraOpParallelism = params.get(TorchModelPredictParams.INTRA_OP_PARALLELISM);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String[] selectedCols = params.get(TorchModelPredictParams.SELECTED_COLS);
		String outputSchemaStr = params.get(TorchModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		String[] reservedCols = params.get(TorchModelPredictParams.RESERVED_COLS);
		//noinspection deprecation
		return Tuple4.of(selectedCols, outputSchema.getFieldNames(), outputSchema.getFieldTypes(), reservedCols);
	}

	private DLPredictorService createPredictor() {
		DLPredictorService predictor = TorchPredictorClassLoaderFactory.create(factory);
		predictor.open(getPredictorConfig());
		return predictor;
	}

	private Map <String, Object> getPredictorConfig() {
		Map <String, Object> config = new HashMap <>();
		config.put(TFSavedModelConstants.MODEL_PATH_KEY, localModelPath);
		config.put(TorchScriptConstants.LIBRARY_PATH_KEY, libraryPath);
		config.put(TFSavedModelConstants.OUTPUT_TYPE_CLASSES, outputColTypeClasses);
		config.put(TFSavedModelConstants.INTRA_OP_PARALLELISM_KEY, intraOpParallelism);
		return config;
	}

	private void destroyPredictor(DLPredictorService predictor) {
		predictor.close();
	}

	@Override
	public void open() {
		String outputSchemaStr = params.get(TorchModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		//noinspection deprecation
		TypeInformation <?>[] tfOutputColTypes = outputSchema.getFieldTypes();
		outputColTypeClasses = Arrays.stream(tfOutputColTypes)
			.map(TypeInformation::getTypeClass)
			.toArray(Class[]::new);

		File workDir = PythonFileUtils.createTempDir("temp_d_").toFile();
		File modelFile = new File(workDir, "model.pt");
		FileDownloadUtils.downloadFile(modelPath, modelFile);
		localModelPath = modelFile.getAbsolutePath();
		libraryPath = new File(LibtorchUtils.getLibtorchPath(LIBTORCH_VERSION), "lib").getAbsolutePath();
		threadLocalPredictor = new CloseableThreadLocal <>(this::createPredictor, this::destroyPredictor);
	}

	@Override
	public void close() {
		LOG.info("counter = {}", counter.sum());
		threadLocalPredictor.close();
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		DLPredictorService predictor = threadLocalPredictor.get();
		long start = System.currentTimeMillis();
		List <Object> inputs = new ArrayList <>();
		for (int i = 0; i < selection.length(); i++) {
			inputs.add(selection.get(i));
		}
		List <?> outputs = predictor.predict(inputs);
		for (int i = 0; i < result.length(); i += 1) {
			result.set(i, outputs.get(i));
		}
		long end = System.currentTimeMillis();
		if (counter.sum() < 100) {
			long elapsed = end - start;
			LOG.info(String.format("Time elapsed for torch inference: %d ms\n", elapsed));
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.printf("Time elapsed for torch inference: %d ms%n", elapsed);
			}
		}
		counter.increment();
	}
}
