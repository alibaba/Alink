/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.tensorflow2.util;

import com.alibaba.flink.ml.cluster.MLConfig;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.proto.framework.ConfigProto;
import org.tensorflow.proto.framework.MetaGraphDef;
import org.tensorflow.proto.framework.SignatureDef;
import org.tensorflow.proto.framework.TensorInfo;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * do tensorflow inference through tensorflow java api.
 * 1. load tensorflow model.
 * 2. load input and output meta.
 */
public class JavaInference implements Closeable {

	private static Logger LOG = LoggerFactory.getLogger(JavaInference.class);
	private static final String SPLITTER = ",";
	private static final String TAG = "serve";

	private final String[] inRowFieldNames;
	private final SavedModelBundle model;
	private final SignatureDef modelSig;
	private final Set<String> inputTensorNameSet;
	private final String[] outputTensorNames;
	private final String[] outputFieldNames;
	private File downloadModelPath;

	public JavaInference(MLConfig mlConfig, TableSchema inSchema, TableSchema outSchema) throws Exception {
		this(mlConfig.getProperties(), inSchema.getFieldNames(), outSchema.getFieldNames());
	}

	public JavaInference(Map<String, String> props, String[] inRowFieldNames, String[] outRowFieldNames)
			throws Exception {
		this.inRowFieldNames = inRowFieldNames;

		// load model
		String exportDir = requireConfig(props, TFConstants.TF_INFERENCE_EXPORT_PATH);
		Path modelPath = new Path(exportDir);
		String scheme = modelPath.toUri().getScheme();
		ConfigProto configProto = ConfigProto.newBuilder()
				.setAllowSoftPlacement(true) // allow less GPUs than configured
				.build();
		// local fs is assumed when no scheme provided
		if (StringUtils.isEmpty(scheme) || scheme.equals("file")) {
			//model = SavedModelBundle.load(exportDir, TAG);
			model = SavedModelBundle.loader(exportDir)
					.withConfigProto(configProto)
					.withTags(TAG)
					.load();
		} else if (scheme.equals("hdfs")) {
			// download the model from hdfs
			FileSystem fs = modelPath.getFileSystem(new Configuration());
			downloadModelPath = Files.createTempDir();
			Path localPath = new Path(downloadModelPath.getPath(), modelPath.getName());
			LOG.info("Downloading model from {} to {}", modelPath, localPath);
			fs.copyToLocalFile(modelPath, localPath);
			//model = SavedModelBundle.load(localPath.toString(), TAG);
			model = SavedModelBundle.loader(localPath.toString())
					.withConfigProto(configProto)
					.withTags(TAG)
					.load();
		} else {
			throw new IllegalArgumentException("Model URI not supported: " + exportDir);
		}
		modelSig = MetaGraphDef.parseFrom(model.metaGraphDef().toByteArray()).getSignatureDefOrThrow("serving_default");
		logSignature();

		// input tensor names must exist in the model and input TableSchema
		String[] inputTensorNames = requireConfig(props, TFConstants.TF_INFERENCE_INPUT_TENSOR_NAMES).split(SPLITTER);
		Preconditions.checkArgument(modelSig.getInputsMap().keySet().containsAll(Arrays.asList(inputTensorNames)) &&
						Arrays.asList(inRowFieldNames).containsAll(Arrays.asList(inputTensorNames)),
				"Invalid input tensor names: " + Arrays.toString(inputTensorNames));
		inputTensorNameSet = new HashSet<>(Arrays.asList(inputTensorNames));

		// output tensor names must exist in the model
		outputTensorNames = requireConfig(props, TFConstants.TF_INFERENCE_OUTPUT_TENSOR_NAMES).split(SPLITTER);
		Preconditions.checkArgument(modelSig.getOutputsMap().keySet().containsAll(Arrays.asList(outputTensorNames)),
				"Invalid output tensor names: " + Arrays.toString(outputTensorNames));

		// output field names must exist in either output tensor names or input TableSchema
		outputFieldNames = requireConfig(props, TFConstants.TF_INFERENCE_OUTPUT_ROW_FIELDS).split(SPLITTER);
		Preconditions
				.checkArgument(outputFieldNames.length == outRowFieldNames.length, "Output fields length mismatch");
		for (String name : outputFieldNames) {
			boolean found = Arrays.asList(outputTensorNames).contains(name) ||
					Arrays.asList(inRowFieldNames).contains(name);
			Preconditions.checkArgument(found, "Unknown output field name: " + name);
		}
	}

	public Row[] generateRowsOneBatch(List<Object[]> batchCache, int batchSize) {
		int size = Math.min(batchSize, batchCache.size());
		if (size <= 0) {
			return new Row[0];
		}
		return generateRows(batchCache, size);
	}

	private Row[] generateRows(List<Object[]> batchCache, int size) {
		Row[] rows = new Row[size];
		Map<String, Object[]> inNameToObjs = new HashMap<>(inRowFieldNames.length);
		for (int i = 0; i < inRowFieldNames.length; i++) {
			inNameToObjs.put(inRowFieldNames[i], extractCols(batchCache, i, size));
		}
		List<Tensor<?>> toClose = new ArrayList<>(inputTensorNameSet.size() + outputTensorNames.length);
		try {
			final Session.Runner runner = model.session().runner();
			for (int i = 0; i < inRowFieldNames.length; i++) {
				if (inputTensorNameSet.contains(inRowFieldNames[i])) {
					// this field is an input tensor
					TensorInfo inputInfo = modelSig.getInputsMap().get(inRowFieldNames[i]);
					Tensor<?> tensor = TFTensorConversion.toTensor(inNameToObjs.get(inRowFieldNames[i]), inputInfo);
					toClose.add(tensor);
					runner.feed(inputInfo.getName(), tensor);
				}
			}
			for (String outputTensorName : outputTensorNames) {
				TensorInfo outputInfo = modelSig.getOutputsMap().get(outputTensorName);
				runner.fetch(outputInfo.getName());
			}
			List<Tensor<?>> outTensors = runner.run();
			toClose.addAll(outTensors);
			Map<String, Tensor<?>> outNameToTensor = new HashMap<>();
			for (int i = 0; i < outputTensorNames.length; i++) {
				outNameToTensor.put(outputTensorNames[i], outTensors.get(i));
			}
			for (int i = 0; i < outputFieldNames.length; i++) {
				Object[] cols;
				if (outNameToTensor.containsKey(outputFieldNames[i])) {
					cols = TFTensorConversion.fromTensor(outNameToTensor.get(outputFieldNames[i]));
				} else {
					cols = inNameToObjs.get(outputFieldNames[i]);
				}
				for (int j = 0; j < rows.length; j++) {
					if (rows[j] == null) {
						rows[j] = new Row(outputFieldNames.length);
					}
					rows[j].setField(i, cols[j]);
				}
			}
		} finally {
			for (Tensor<?> tensor : toClose) {
				tensor.close();
			}
		}
		return rows;
	}

	@Override
	public void close() throws IOException {
		if (model != null) {
			model.close();
			LOG.info("Model closed");
		}
		if (downloadModelPath != null) {
			FileUtils.deleteQuietly(downloadModelPath);
		}
	}

	private String requireConfig(Map<String, String> props, String key) {
		String val = props.get(key);
		Preconditions.checkArgument(!StringUtils.isEmpty(val), "Need to specify proper " + key);
		return val;
	}

	private void logSignature() {
		int numInputs = modelSig.getInputsCount();
		StringBuilder builder = new StringBuilder();
		int i = 1;
		builder.append("\nMODEL SIGNATURE\n");
		builder.append("Inputs:\n");
		for (Map.Entry<String, TensorInfo> entry : modelSig.getInputsMap().entrySet()) {
			TensorInfo t = entry.getValue();
			builder.append(String.format(
					"%d of %d: %-20s (Node name in graph: %-20s, type: %s)\n",
					i++, numInputs, entry.getKey(), t.getName(), t.getDtype()));
		}
		int numOutputs = modelSig.getOutputsCount();
		i = 1;
		builder.append("Outputs:\n");
		for (Map.Entry<String, TensorInfo> entry : modelSig.getOutputsMap().entrySet()) {
			TensorInfo t = entry.getValue();
			builder.append(String.format(
					"%d of %d: %-20s (Node name in graph: %-20s, type: %s)\n",
					i++, numOutputs, entry.getKey(), t.getName(), t.getDtype()));
		}
		builder.append("-----------------------------------------------");
		LOG.info(builder.toString());
	}

	private Object[] extractCols(List<Object[]> cache, int index, int len) {
		Object[] res = new Object[len];
		for (int i = 0; i < res.length; i++) {
			res[i] = cache.get(i)[index];
		}
		return res;
	}
}
