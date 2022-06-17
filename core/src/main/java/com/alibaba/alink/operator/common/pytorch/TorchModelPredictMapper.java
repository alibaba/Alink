package com.alibaba.alink.operator.common.pytorch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper;
import com.alibaba.alink.common.dl.plugin.TorchPredictorClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dl.TorchModelPredictParams;

import java.io.File;
import java.util.Arrays;

public class TorchModelPredictMapper extends DLPredictServiceMapper <TorchPredictorClassLoaderFactory> {

	private final static String TORCH_JAVA_VERSION = "1.8.0r1";
	private final static String LIBTORCH_VERSION = "1.8.1";

	private final int intraOpParallelism;
	private Class <?>[] outputColTypeClasses;
	private String libraryPath;

	private final ResourcePluginFactory resourceFactory;

	public TorchModelPredictMapper(TableSchema dataSchema, Params params) {
		this(dataSchema, params, new TorchPredictorClassLoaderFactory(TORCH_JAVA_VERSION));
	}

	public TorchModelPredictMapper(TableSchema dataSchema, Params params, TorchPredictorClassLoaderFactory factory) {
		super(dataSchema, params, factory, false);
		this.intraOpParallelism = params.get(TorchModelPredictParams.INTRA_OP_PARALLELISM);
		resourceFactory = new ResourcePluginFactory();
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

	public PredictorConfig getPredictorConfig() {
		PredictorConfig config = new PredictorConfig();
		config.factory = factory;
		config.modelPath = localModelPath;
		config.libraryPath = libraryPath;
		config.outputTypeClasses = outputColTypeClasses;
		config.intraOpNumThreads = intraOpParallelism;
		config.threadMode = false;
		return config;
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
		String libtorchPath = LibtorchUtils.getLibtorchPath(resourceFactory, LIBTORCH_VERSION);
		libraryPath = new File(libtorchPath, "lib").getAbsolutePath();
		super.open();
	}
}
