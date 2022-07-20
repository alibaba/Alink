package com.alibaba.alink.common.dl.plugin;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.FileUtils;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.FileDownloadUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPluginErrorException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.CloseableThreadLocal;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.codec.binary.Base64;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public abstract class DLPredictServiceMapper<FACTORY extends ClassLoaderFactory> extends Mapper
	implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(DLPredictServiceMapper.class);

	protected final boolean isThreadSafe;
	protected final FACTORY factory;
	protected final String[] outputCols;
	protected final Class <?>[] outputColTypeClasses;
	protected String[] inputCols;
	protected String modelPath;
	protected String localModelPath;
	protected File workDir;

	// when isThreadSafe is true, use predictor, otherwise use threadLocalPredictor
	protected DLPredictorService predictor;
	protected transient CloseableThreadLocal <DLPredictorService> threadLocalPredictor;

	private final LongAdder counter = new LongAdder();

	public DLPredictServiceMapper(TableSchema dataSchema, Params params, FACTORY factory, boolean isThreadSafe) {
		super(dataSchema, params);

		this.factory = factory;
		this.isThreadSafe = isThreadSafe;

		inputCols = params.get(HasSelectedColsDefaultAsNull.SELECTED_COLS);
		if (null == inputCols) {
			inputCols = dataSchema.getFieldNames();
		}

		AkPreconditions.checkArgument(params.contains(HasOutputSchemaStr.OUTPUT_SCHEMA_STR),
			new AkIllegalOperatorParameterException("Must set outputSchemaStr."));
		String outputSchemaStr = params.get(HasOutputSchemaStr.OUTPUT_SCHEMA_STR);
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		outputCols = outputSchema.getFieldNames();

		//noinspection deprecation
		TypeInformation <?>[] outputColTypes = outputSchema.getFieldTypes();
		outputColTypeClasses = Arrays.stream(outputColTypes)
			.map(TypeInformation::getTypeClass)
			.toArray(Class[]::new);

		if (params.contains(HasModelPath.MODEL_PATH)) {
			modelPath = params.get(HasModelPath.MODEL_PATH);
		}
	}

	public DLPredictServiceMapper <FACTORY> setModelPath(String modelPath) {
		this.modelPath = modelPath;
		return this;
	}

	protected abstract PredictorConfig getPredictorConfig();

	protected DLPredictorService createPredictor() {
		ClassLoader classLoader = factory.create();
		DLPredictorService predictor;
		try {
			Method createMethod = factory.getClass().getMethod("create", factory.getClass());
			predictor = (DLPredictorService) createMethod.invoke(null, factory);
		} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
			throw new AkPluginErrorException(
				String.format("Failed to call %s#create(factory).", factory.getClass().getCanonicalName()), e);
		}
		try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
			predictor.open(getPredictorConfig());
		}
		return predictor;
	}

	protected void destroyPredictor(DLPredictorService predictor) {
		try {
			predictor.close();
		} catch (Exception e) {
			throw new AkUnclassifiedErrorException("Failed to close predictor", e);
		}
	}

	@Override
	public void open() {
		AkPreconditions.checkArgument(modelPath != null, "Model path is not set.");
		workDir = PythonFileUtils.createTempDir("temp_d_").toFile();
		File modelFile = new File(workDir, "model");
		FileDownloadUtils.downloadFile(modelPath, modelFile);
		localModelPath = modelFile.getAbsolutePath();
		if (isThreadSafe) {
			predictor = createPredictor();
		} else {
			threadLocalPredictor = new CloseableThreadLocal <>(this::createPredictor, this::destroyPredictor);
		}
	}

	@Override
	public void close() {
		if (isThreadSafe) {
			destroyPredictor(predictor);
		} else {
			threadLocalPredictor.close();
		}
		FileUtils.deleteDirectoryQuietly(workDir);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String[] inputCols = params.get(HasSelectedColsDefaultAsNull.SELECTED_COLS);
		if (null == inputCols) {
			inputCols = dataSchema.getFieldNames();
		}
		String outputSchemaStr = params.get(HasOutputSchemaStr.OUTPUT_SCHEMA_STR);
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		String[] reservedCols = params.get(HasReservedColsDefaultAsNull.RESERVED_COLS);
		//noinspection deprecation
		return Tuple4.of(inputCols, outputSchema.getFieldNames(), outputSchema.getFieldTypes(), reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		DLPredictorService predictor = isThreadSafe
			? this.predictor
			: threadLocalPredictor.get();

		long start = System.currentTimeMillis();
		List <Object> inputs = new ArrayList <>();
		for (int i = 0; i < selection.length(); i += 1) {
			inputs.add(selection.get(i));
		}
		List <?> outputs = predictor.predict(inputs);
		for (int i = 0; i < result.length(); i += 1) {
			result.set(i, outputs.get(i));
		}
		long end = System.currentTimeMillis();
		if (counter.sum() < 100) {
			long elapsed = end - start;
			String s = String.format("Time elapsed for %s inference: %d ms",
				predictor.getClass().getSimpleName(), elapsed);
			LOG.info(s);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(s);
			}
		}
		counter.increment();
	}

	public static class PredictorConfig {
		public ClassLoaderFactory factory;

		public String modelPath;
		public Class <?>[] outputTypeClasses;

		public String[] inputNames;
		public String[] outputNames;
		public Integer intraOpNumThreads;
		public Integer interOpNumThreads;
		public Integer cudaDeviceNum;
		public boolean threadMode = true;

		// for PyTorch
		public String libraryPath;

		// create Kryo instance every time to make it thread-safe.
		static Kryo newKryoInstance() {
			Kryo kryo = new Kryo();
			kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
			kryo.register(PluginDistributeCache.class, new Serializer <PluginDistributeCache>() {

				@Override
				public void write(Kryo kryo, Output output, PluginDistributeCache object) {
					Map <String, String> context = new HashMap <>(object.context());
					kryo.writeClassAndObject(output, context);
				}

				@Override
				public PluginDistributeCache read(Kryo kryo, Input input, Class <PluginDistributeCache> type) {
					//noinspection unchecked
					Map <String, String> context = (Map <String, String>) kryo.readClassAndObject(input);
					return new PluginDistributeCache(context);
				}
			});
			return kryo;
		}

		// Define conversion methods for compatibility with previous definition.
		// NOTE: `fromMap` only supports maps obtained from `toMap`.
		public String serialize() {
			Kryo kryo = newKryoInstance();
			Output output = new Output(1, Integer.MAX_VALUE);
			kryo.writeClassAndObject(output, this);
			byte[] bytes = output.toBytes();
			return Base64.encodeBase64String(bytes);
		}

		synchronized public static PredictorConfig deserialize(String s) {
			Kryo kryo = newKryoInstance();
			byte[] bytes = Base64.decodeBase64(s);
			Input input = new Input(bytes);
			return (PredictorConfig) kryo.readClassAndObject(input);
		}
	}
}
