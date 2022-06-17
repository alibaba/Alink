package com.alibaba.alink.common.dl.plugin;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper.PredictorConfig;
import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtSession.Result;
import ai.onnxruntime.OrtSession.SessionOptions;
import ai.onnxruntime.TensorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class OnnxJavaPredictor implements DLPredictorService, Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(OnnxJavaPredictor.class);

	OrtEnvironment env;
	OrtSession session;

	private Class <?>[] outputTypeClasses;
	private String[] inputOpNames;
	private String[] outputOpNames;
	private TensorInfo[] inputTensorInfos;

	private static Tensor <?> scalarToTensor(Object v) {
		if (v instanceof Integer) {
			return new IntTensor((Integer) v);
		} else if (v instanceof Long) {
			return new LongTensor((Long) v);
		} else if (v instanceof Float) {
			return new FloatTensor((Float) v);
		} else if (v instanceof Double) {
			return new DoubleTensor((Double) v);
		} else if (v instanceof Boolean) {
			return new BoolTensor((Boolean) v);
		} else if (v instanceof Byte) {
			return new ByteTensor((Byte) v);
		} else {
			throw new IllegalArgumentException(String.format("%s is not a primitive scalar.", v));
		}
	}

	private static OnnxTensor objectToTensor(OrtEnvironment env, Object v, TensorInfo tensorInfo) {
		Tensor <?> tensor;
		try {
			tensor = TensorUtil.getTensor(v);
		} catch (Exception e) {
			tensor = scalarToTensor(v);
		}
		try {
			return OnnxTensorConversionUtils.toOnnxTensor(env, tensor, tensorInfo);
		} catch (OrtException e) {
			throw new RuntimeException(String.format("Failed to convert Alink tensor to ONNX tensor: %s", tensor), e);
		}
	}

	private static Object tensorToObject(OnnxTensor onnxTensor, Class <?> clazz) {
		Tensor <?> tensor;
		try {
			tensor = OnnxTensorConversionUtils.fromOnnxTensor(onnxTensor);
		} catch (OrtException e) {
			throw new RuntimeException(String.format("Failed to convert ONNX tensor to Alink tensor: %s", onnxTensor),
				e);
		}
		if (Tensor.class.isAssignableFrom(clazz)) {
			return tensor;
		}
		// Allow size-1 tensor to be converted to primitives.
		if (tensor.size() == 1) {
			Object v = tensor.flatten().getObject(0);
			if (clazz.isAssignableFrom(v.getClass())) {
				return v;
			} else {
				throw new RuntimeException(
					String.format("Value %s cannot be assigned to type %s.", v, clazz.getSimpleName()));
			}
		} else {
			throw new IllegalArgumentException("Cannot convert multiple values to primitive scalars.");
		}
	}

	@Override
	public void open(PredictorConfig config) {
		String modelPath = config.modelPath;
		String[] inputNames = config.inputNames;
		String[] outputNames = config.outputNames;
		Integer intraOpNumThreads = config.intraOpNumThreads;
		Integer interOpNumThreads = config.interOpNumThreads;
		outputTypeClasses = config.outputTypeClasses;
		Integer cudaDeviceNum = config.cudaDeviceNum;
		if (null == intraOpNumThreads) {
			intraOpNumThreads = 0;
		}
		if (null == interOpNumThreads) {
			interOpNumThreads = 0;
		}

		env = OrtEnvironment.getEnvironment();
		try {
			SessionOptions sessionOptions = new SessionOptions();
			sessionOptions.setInterOpNumThreads(interOpNumThreads);
			sessionOptions.setIntraOpNumThreads(intraOpNumThreads);
			if (null != cudaDeviceNum) {
				sessionOptions.addCUDA(cudaDeviceNum);
			}
			session = env.createSession(modelPath, sessionOptions);
		} catch (OrtException e) {
			throw new RuntimeException("Failed to create session.", e);
		}

		Map <String, NodeInfo> inputInfo;
		Map <String, NodeInfo> outputInfo;
		try {
			inputInfo = session.getInputInfo();
			outputInfo = session.getOutputInfo();
		} catch (OrtException e) {
			throw new RuntimeException("Cannot obtain input/output info from ONNX model.", e);
		}

		inputInfo.forEach((k, v) -> LOG.info(
			"Input " + k + ": " + v.getName() + ", " + ((TensorInfo) v.getInfo()).type + ", " + Arrays.toString(
				((TensorInfo) v.getInfo()).getShape())));
		outputInfo.forEach((k, v) -> LOG.info(
			"Output " + k + ": " + v.getName() + ", " + ((TensorInfo) v.getInfo()).type + ", " + Arrays.toString(
				((TensorInfo) v.getInfo()).getShape())));
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			inputInfo.forEach((k, v) -> System.out.println(
				"Input " + k + ": " + v.getName() + ", " + ((TensorInfo) v.getInfo()).type + ", " + Arrays.toString(
					((TensorInfo) v.getInfo()).getShape())));
			outputInfo.forEach((k, v) -> System.out.println(
				"Output " + k + ": " + v.getName() + ", " + ((TensorInfo) v.getInfo()).type + ", " + Arrays.toString(
					((TensorInfo) v.getInfo()).getShape())));
		}

		Preconditions.checkArgument(inputInfo.size() == inputNames.length, "Incorrect number of inputs");
		inputOpNames = new String[inputNames.length];
		outputOpNames = new String[outputNames.length];
		inputTensorInfos = new TensorInfo[inputNames.length];

		for (int i = 0; i < inputNames.length; i += 1) {
			NodeInfo nodeInfo = inputInfo.get(inputNames[i]);
			Preconditions.checkArgument(nodeInfo != null, "Input name not exist: " + inputNames[i]);
			Preconditions.checkArgument(nodeInfo.getInfo() instanceof TensorInfo,
				String.format("Input name %s is not a tensor.", inputNames[i]));
			inputTensorInfos[i] = (TensorInfo) nodeInfo.getInfo();
			inputOpNames[i] = nodeInfo.getName();
		}

		for (int i = 0; i < outputNames.length; i += 1) {
			NodeInfo nodeInfo = outputInfo.get(outputNames[i]);
			Preconditions.checkArgument(nodeInfo != null, "Output name not exist: " + outputNames[i]);
			Preconditions.checkArgument(nodeInfo.getInfo() instanceof TensorInfo,
				String.format("Output name %s is not a tensor (Sequences/Maps are not supported yet.).",
					inputNames[i]));
			outputOpNames[i] = nodeInfo.getName();
		}
	}

	public void close() {
		if (null != session) {
			try {
				session.close();
			} catch (OrtException e) {
				throw new RuntimeException("Failed to close session.", e);
			}
		}
		if (null != env) {
			env.close();
		}
	}

	List <OnnxTensor> doPredict(List <OnnxTensor> tensors) {
		Map <String, OnnxTensor> inputMap = new HashMap <>();
		for (int i = 0; i < inputOpNames.length; i += 1) {
			inputMap.put(inputOpNames[i], tensors.get(i));
		}
		List <OnnxValue> outputValues = new ArrayList <>();
		Result result;
		try {
			result = session.run(inputMap);
		} catch (OrtException e) {
			throw new RuntimeException("Failed to run inference.", e);
		}
		for (String outputOpName : outputOpNames) {
			Optional <OnnxValue> optionalOnnxValue = result.get(outputOpName);
			Preconditions.checkArgument(optionalOnnxValue.isPresent());
			outputValues.add(optionalOnnxValue.get());
		}
		// TODO: assume all outputs are OnnxTensors for now
		return outputValues.stream().map(d -> (OnnxTensor) d).collect(Collectors.toList());
	}

	@Override
	public List <?> predict(List <?> inputs) {
		List <OnnxTensor> tensors = new ArrayList <>(inputTensorInfos.length);
		for (int i = 0; i < inputTensorInfos.length; i += 1) {
			Object v = inputs.get(i);
			tensors.add(objectToTensor(env, v, inputTensorInfos[i]));
		}
		List <OnnxTensor> outputTensors = doPredict(tensors);
		List <Object> outputs = new ArrayList <>();
		for (int i = 0; i < inputTensorInfos.length; i += 1) {
			outputs.add(tensorToObject(outputTensors.get(i), outputTypeClasses[i]));
		}
		tensors.forEach(OnnxTensor::close);
		outputTensors.forEach(OnnxTensor::close);
		return outputs;
	}

	@Override
	public List <List <?>> predictRows(List <List <?>> inputs, int batchSize) {
		throw new UnsupportedOperationException("Not supported batch inference yet.");
	}
}
