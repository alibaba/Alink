package com.alibaba.flink.ml.tensorflow.io.descriptor;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.descriptors.DescriptorProperties.noValidation;

public class TFRToRowTableValidator extends ConnectorDescriptorValidator {
    public static final String CONNECTOR_PATH = "connector.path";
    public static final String CONNECTOR_FILES = "connector.files";
    public static final String CONNECTOR_EPOCHS = "connector.epochs";
    public static final String CONNECTOR_CONVERTERS = "connector.converters";
    public static final String CONNECTOR_OUT_COL_ALIASES = "connector.out-col-aliases";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        if (properties.containsKey(CONNECTOR_CONVERTERS)) {
            validateConverters(properties);
        }
    }

    private void validateConverters(DescriptorProperties properties) {
        Map<String, Consumer<String>> convertersValidation = new HashMap<>();
        convertersValidation.put("first", noValidation());
        convertersValidation.put("last", noValidation());
        convertersValidation.put("max", noValidation());
        convertersValidation.put("min", noValidation());
        convertersValidation.put("one_hot", noValidation());

        properties.validateEnum(CONNECTOR_CONVERTERS, false, convertersValidation);
    }
}
