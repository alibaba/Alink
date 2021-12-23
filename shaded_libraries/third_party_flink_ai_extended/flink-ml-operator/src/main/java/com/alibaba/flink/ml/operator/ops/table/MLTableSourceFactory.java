package com.alibaba.flink.ml.operator.ops.table;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.role.AMRole;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import com.alibaba.flink.ml.cluster.role.PsRole;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.ml.operator.ops.table.descriptor.MLTableValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

public class MLTableSourceFactory implements TableSourceFactory<Row> {
    @Override
    public TableSource<Row> createTableSource(Context context) {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(context.getTable().toProperties());

        TableSchema tableSchema = context.getTable().getSchema();
        ExecutionMode mode = getExecutionMode(properties);
        BaseRole role = getRole(properties);
        MLConfig mlConfig = getMLConfig(properties);
        int parallelism = getParallelism(properties);
        return new MLTableSource(mode, role, mlConfig, tableSchema, parallelism);
    }

    private int getParallelism(DescriptorProperties properties) {
        return properties.getInt(CONNECTOR_PARALLELISM);
    }

    private MLConfig getMLConfig(DescriptorProperties properties) {
        Map<String, String> mlConfigProperties = properties.getPropertiesWithPrefix(CONNECTOR_ML_CONFIG_PROPERTIES);
        Map<String, String> mlRoleParallelismProperties = properties.getPropertiesWithPrefix(CONNECTOR_ML_CONFIG_ROLE_PARALLELISM_MAP);
        Map<String, Integer> mlRoleParallelismMap = toMlRoleParallelismMap(mlRoleParallelismProperties);
        String funcName = properties.getString(CONNECTOR_ML_CONFIG_FUNC_NAME);
        String envPath = null;
        if (properties.containsKey(CONNECTOR_ML_CONFIG_ENV_PATH)) {
            envPath = properties.getString(CONNECTOR_ML_CONFIG_ENV_PATH);
        }

        String[] pythonFiles = properties
                .getFixedIndexedProperties(CONNECTOR_ML_CONFIG_PYTHON_FILES, Collections.singletonList("name"))
                .stream().map(m -> m.get("name")).map(properties::getString).toArray(String[]::new);
        return new MLConfig(mlRoleParallelismMap, mlConfigProperties, pythonFiles, funcName, envPath);

    }

    private Map<String, Integer> toMlRoleParallelismMap(Map<String, String> mlRoleParallelismProperties) {
        Map<String, Integer> res = new HashMap<>();
        mlRoleParallelismProperties.forEach((k, v) -> res.put(k, Integer.valueOf(v)));
        return res;
    }

    private BaseRole getRole(DescriptorProperties properties) {
        String roleClass = properties.getString(CONNECTOR_ROLE_CLASS);
        try {
            return (BaseRole) Class.forName(roleClass).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ExecutionMode getExecutionMode(DescriptorProperties properties) {
        return ExecutionMode.valueOf(properties.getString(CONNECTOR_EXECUTION_MODE));
    }

    @Override
    public Map<String, String> requiredContext() {
        return Collections.singletonMap(CONNECTOR_TYPE, "MLTable");
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.singletonList("*");
    }
}
