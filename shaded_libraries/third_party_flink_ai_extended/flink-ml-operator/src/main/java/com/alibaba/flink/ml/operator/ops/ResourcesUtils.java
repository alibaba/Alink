package com.alibaba.flink.ml.operator.ops;

import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.RuntimeContext;

import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.util.MLConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ResourcesUtils {

	public static void parseGpuInfo(RuntimeContext runtimeContext, MLConfig mlConfig) {
		Set<ExternalResourceInfo> gpuInfo = runtimeContext.getExternalResourceInfos("gpu");
		if (gpuInfo != null && gpuInfo.size() >0) {
			List<String> indexList = new ArrayList<>();
			for (ExternalResourceInfo gpu : gpuInfo) {
				if (gpu.getProperty("index").isPresent()) {
					indexList.add(gpu.getProperty("index").get());
				}
			}
			Collections.sort(indexList);
			String gpuStr = String.join(",", indexList);
			mlConfig.getProperties().put(MLConstants.GPU_INFO, gpuStr);
		}else {
			mlConfig.getProperties().put(MLConstants.GPU_INFO, "");
		}
	}
}
