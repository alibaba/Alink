package com.alibaba.flink.ml.operator.ops.table.descriptor;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.descriptors.DescriptorProperties.noValidation;

public class MLTableValidator extends ConnectorDescriptorValidator {

    public static final String CONNECTOR_ROLE_CLASS = "connector.role.class";
    public static final String CONNECTOR_EXECUTION_MODE = "connector.execution-mode";
    public static final String CONNECTOR_PARALLELISM = "connector.parallelism";
    public static final String CONNECTOR_ML_CONFIG_PROPERTIES = "connector.ml-config.properties";
    public static final String CONNECTOR_ML_CONFIG_ROLE_PARALLELISM_MAP = "connector.ml-config.role-parallelism-map";
    public static final String CONNECTOR_ML_CONFIG_FUNC_NAME = "connector.ml-config.func-name";
    public static final String CONNECTOR_ML_CONFIG_PYTHON_FILES = "connector.ml-config.python-files";
    public static final String CONNECTOR_ML_CONFIG_ENV_PATH = "connector.ml-config.env-path";


    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);

        validateExecutionMode(properties);
    }

    private void validateExecutionMode(DescriptorProperties properties) {
        Map<String, Consumer<String>> executionModeValidation = new HashMap<>();
        executionModeValidation.put("train", noValidation());
        executionModeValidation.put("inference", noValidation());
        executionModeValidation.put("other", noValidation());

        properties.validateEnum(CONNECTOR_EXECUTION_MODE, false, executionModeValidation);
    }
}
