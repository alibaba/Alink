package com.alibaba.flink.ml.operator.ops.table.descriptor;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.*;

import static com.alibaba.flink.ml.operator.ops.table.descriptor.MLTableValidator.*;

public class MLTable extends ConnectorDescriptor {

    DescriptorProperties properties = new DescriptorProperties();

    public MLTable() {
        super("MLTable", 1, false);
    }

    public MLTable mlConfig(MLConfig mlConfig) {
        properties.putPropertiesWithPrefix(CONNECTOR_ML_CONFIG_PROPERTIES, mlConfig.getProperties());
        Map<String, String> roleParallelismProperties = roleParallelismMapToProperties(mlConfig.getRoleParallelismMap());
        properties.putPropertiesWithPrefix(CONNECTOR_ML_CONFIG_ROLE_PARALLELISM_MAP, roleParallelismProperties);
        properties.putString(CONNECTOR_ML_CONFIG_FUNC_NAME, mlConfig.getFuncName());

        if (mlConfig.getEnvPath() != null) {
            properties.putString(CONNECTOR_ML_CONFIG_ENV_PATH, mlConfig.getEnvPath());
        }

        List<List<String>> pythonFilesIndexedProperties = pythonFilesToIndexedProperties(mlConfig.getPythonFiles());
        properties.putIndexedFixedProperties(CONNECTOR_ML_CONFIG_PYTHON_FILES, Collections.singletonList("name"),
                pythonFilesIndexedProperties);
        return this;
    }

    private List<List<String>> pythonFilesToIndexedProperties(String[] pythonFiles) {
        List<List<String>> res = new ArrayList<>();
        for (String file : pythonFiles) {
            res.add(Collections.singletonList(file));
        }
        return res;
    }

    private Map<String, String> roleParallelismMapToProperties(Map<String, Integer> roleParallelismMap) {
        Map<String, String> res = new HashMap<>();
        roleParallelismMap.forEach((k, v) -> res.put(k, String.valueOf(v)));
        return res;
    }

    public MLTable executionMode (ExecutionMode executionMode) {
        properties.putString(CONNECTOR_EXECUTION_MODE, executionMode.name());
        return this;
    }

    public MLTable role(BaseRole role) {
        properties.putString(CONNECTOR_ROLE_CLASS, role.getClass().getCanonicalName());
        return this;
    }

    public MLTable parallelism(int parallelism) {
        properties.putInt(CONNECTOR_PARALLELISM, parallelism);
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return properties.asMap();
    }
}
