package com.alibaba.alink.common.dl.plugin;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.dl.utils.TF2TensorUtils;
import com.alibaba.alink.common.dl.utils.tftensorconv.StringTFTensorConversionImpl;
import com.alibaba.alink.common.dl.utils.tftensorconv.TensorTFTensorConversionImpl;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.proto.framework.ConfigProto;
import org.tensorflow.proto.framework.GPUOptions;
import org.tensorflow.proto.framework.MetaGraphDef;
import org.tensorflow.proto.framework.SignatureDef;
import org.tensorflow.proto.framework.TensorInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.GRAPH_DEF_TAG_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INPUT_SIGNATURE_DEFS_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTER_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTRA_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.MODEL_PATH_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_BATCH_AXES;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_SIGNATURE_DEFS_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_TYPE_CLASSES;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.SIGNATURE_DEF_KEY_KEY;

public class TFPredictorServiceImpl implements DLPredictorService {

	private Class <?>[] outputTypeClasses;
	private SavedModelBundle model;
	private String[] inputOpNames;
	private String[] outputOpNames;
	private TensorInfo[] inputTensorInfos;

	// Indicate batch dimensions indices of the output tensors.
	private int[] outputBatchAxes;

	/**
	 * Normalize value so {@link TF2TensorUtils#parseTensor} or {@link TF2TensorUtils#parseBatchTensors} can apply
	 * correctly.
	 *
	 * @param v
	 * @return
	 */
	private static Object normalizeValue(Object v) {
		// from serialization strings of Alink Tensor
		if (v instanceof String) {
			try {
				v = TensorUtil.parseTensor((String) v);
			} catch (Exception ignored) {
			}
		}
		// from values of primitive types
		if (!(v instanceof com.alibaba.alink.common.linalg.tensor.Tensor)) {
			v = String.valueOf(v);
		}
		return v;
	}

	private static Tensor <?> objectToTensor(Object v, TensorInfo tensorInfo) {
		v = normalizeValue(v);
		return TF2TensorUtils.parseTensor(v, tensorInfo);
	}

	private static Object tensorToObject(Tensor <?> tensor, Class <?> clazz) {
		// to Alink Tensor
		if (com.alibaba.alink.common.linalg.tensor.Tensor.class.isAssignableFrom(clazz)) {
			return TensorTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		}
		String s = StringTFTensorConversionImpl.getInstance().encodeTensor(tensor);
		// to primitive types or String
		return clazz.equals(String.class) ? s : JsonConverter.fromJson(s, clazz);
	}

	private static List <Object> tensorToObjectList(Tensor <?> tensor, Class <?> clazz, int batchSize, int batchAxis) {
		// to Alink Tensor
		if (com.alibaba.alink.common.linalg.tensor.Tensor.class.isAssignableFrom(clazz)) {
			com.alibaba.alink.common.linalg.tensor.Tensor <?>[] tensors
				= TensorTFTensorConversionImpl.getInstance().encodeBatchTensor(tensor, batchAxis);
			return Arrays.asList(tensors);
		}
		// to primitive types or String
		List <Object> values = new ArrayList <>();
		String[] s = StringTFTensorConversionImpl.getInstance().encodeBatchTensor(tensor, batchAxis);
		for (int i = 0; i < batchSize; i += 1) {
			Object v = clazz.equals(String.class) ? s[i] : JsonConverter.fromJson(s[i], clazz);
			values.add(v);
		}
		return values;
	}

	@Override
	public void open(Map <String, Object> config) {
		String modelPath = (String) config.get(MODEL_PATH_KEY);
		String graphDefTag = (String) config.get(GRAPH_DEF_TAG_KEY);
		String signatureDefKey = (String) config.get(SIGNATURE_DEF_KEY_KEY);
		String[] inputNames = (String[]) config.get(INPUT_SIGNATURE_DEFS_KEY);
		String[] outputNames = ((String[]) config.get(OUTPUT_SIGNATURE_DEFS_KEY));
		Integer intraOpParallelism = (Integer) config.get(INTRA_OP_PARALLELISM_KEY);
		Integer interOpParallelism = (Integer) config.get(INTER_OP_PARALLELISM_KEY);
		outputTypeClasses = (Class <?>[]) config.get(OUTPUT_TYPE_CLASSES);
		outputBatchAxes = (int[]) config.getOrDefault(OUTPUT_BATCH_AXES, new int[outputTypeClasses.length]);

		ConfigProto.Builder configProtoBuilder = ConfigProto.newBuilder();
		configProtoBuilder.setAllowSoftPlacement(true)
			.setGpuOptions(GPUOptions.newBuilder().setAllowGrowth(true).build());
		if (null != intraOpParallelism) {
			configProtoBuilder.setIntraOpParallelismThreads(intraOpParallelism);
		}
		if (null != interOpParallelism) {
			configProtoBuilder.setInterOpParallelismThreads(interOpParallelism);
		}

		model = SavedModelBundle.loader(modelPath)
			.withTags(graphDefTag)
			.withConfigProto(configProtoBuilder.build())
			.load();

		MetaGraphDef m = model.metaGraphDef();
		SignatureDef sig = m.getSignatureDefOrThrow(signatureDefKey);
		Map <String, TensorInfo> inputsMap = sig.getInputsMap();
		Map <String, TensorInfo> outputsMap = sig.getOutputsMap();

		inputsMap.forEach((k, v) ->
			System.out.println(k + " ---- " + v.getName() + ", " + v.getDtype() + ", " +
				Arrays.toString(TF2TensorUtils.getTensorShape(v))));
		outputsMap.forEach((k, v) ->
			System.out.println(k + " ---- " + v.getName() + ", " + v.getDtype() + ", " +
				Arrays.toString(TF2TensorUtils.getTensorShape(v))));

		Preconditions.checkArgument(inputsMap.size() == inputNames.length, "Incorrect number of inputs");
		inputOpNames = new String[inputNames.length];
		outputOpNames = new String[outputNames.length];
		inputTensorInfos = new TensorInfo[inputNames.length];
		TensorInfo[] outputTensorInfos = new TensorInfo[outputNames.length];

		for (int i = 0; i < inputNames.length; i++) {
			inputTensorInfos[i] = inputsMap.get(inputNames[i]);
			Preconditions.checkArgument(inputTensorInfos[i] != null, "Input name not exist: " + inputNames[i]);
			inputOpNames[i] = inputTensorInfos[i].getName();
			System.out.println("shape of " + inputNames[i] + ": " +
				Arrays.toString(TF2TensorUtils.getTensorShape(inputTensorInfos[i])));
		}

		for (int i = 0; i < outputNames.length; i++) {
			outputTensorInfos[i] = outputsMap.get(outputNames[i]);
			Preconditions.checkArgument(outputTensorInfos[i] != null, "Output name not exist: " + outputNames[i]);
			outputOpNames[i] = outputTensorInfos[i].getName();
			System.out.println("shape of " + outputNames[i] + ": " +
				Arrays.toString(TF2TensorUtils.getTensorShape(outputTensorInfos[i])));
		}
	}

	@Override
	public void close() {
		if (null != model) {
			model.close();
		}
	}

	private List <Tensor <?>> doPredict(List <Tensor <?>> tensors) {
		Session.Runner runner = this.model.session().runner();
		for (int i = 0; i < inputOpNames.length; i++) {
			runner.feed(inputOpNames[i], tensors.get(i));
		}
		for (String outputOpName : outputOpNames) {
			runner.fetch(outputOpName);
		}
		return runner.run();
	}

	private void releaseTensors(List <Tensor <?>> tensors) {
		for (Tensor <?> tensor : tensors) {
			tensor.close();
		}
	}

	/**
	 * Predict with values representing input tensors and produce values representing output tensors.
	 * <p>
	 * Their orders are the same with `inputTensorInfos` and `outputTensorInfos`, respectively. Their values should be
	 * convertible from/to Tensor through {@link TFPredictorServiceImpl#objectToTensor} and {@link
	 * TFPredictorServiceImpl#tensorToObject}.
	 *
	 * @param inputs values representing input tensors.
	 * @return prediction results.
	 */
	@Override
	public List <?> predict(List <?> inputs) {
		List <Tensor <?>> tensors = new ArrayList <>(inputTensorInfos.length);
		for (int i = 0; i < inputTensorInfos.length; i += 1) {
			Object v = inputs.get(i);
			tensors.add(objectToTensor(v, inputTensorInfos[i]));
		}
		List <Tensor <?>> outputTensors = doPredict(tensors);
		List <Object> outputs = new ArrayList <>();
		for (int i = 0; i < outputTypeClasses.length; i += 1) {
			outputs.add(tensorToObject(outputTensors.get(i), outputTypeClasses[i]));
		}
		releaseTensors(tensors);
		releaseTensors(outputTensors);
		return outputs;
	}

	@Override
	public List <List <?>> predictRows(List <List <?>> inputs, int batchSize) {
		List <Tensor <?>> tensors = new ArrayList <>();
		for (int k = 0; k < inputTensorInfos.length; k += 1) {
			List <Object> valueList = inputs.get(k)
				.stream().map(TFPredictorServiceImpl::normalizeValue)
				.collect(Collectors.toList());
			tensors.add(TF2TensorUtils.parseBatchTensors(valueList, inputTensorInfos[k]));
		}
		List <Tensor <?>> outputTensors = doPredict(tensors);

		List <List <?>> results = new ArrayList <>();
		for (int k = 0; k < outputTensors.size(); k += 1) {
			Tensor <?> tensor = outputTensors.get(k);
			Class <?> tfOutputColTypeClass = outputTypeClasses[k];
			results.add(tensorToObjectList(tensor, tfOutputColTypeClass, batchSize, outputBatchAxes[k]));
		}

		releaseTensors(tensors);
		releaseTensors(outputTensors);
		return results;
	}
}
