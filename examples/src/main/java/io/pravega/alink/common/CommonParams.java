package io.pravega.alink.common;

import org.apache.flink.api.java.utils.ParameterTool;

import java.net.URI;

// All parameters will come from environment variables. This makes it easy
// to configure on Docker, Mesos, Kubernetes, etc.
public class CommonParams {
    // By default, we will connect to a standalone Pravega running on localhost.
    public static URI getControllerURI() {
        return URI.create(getEnvVar("PRAVEGA_CONTROLLER_URI", "tcp://localhost:9090"));
    }

    public static boolean isPravegaStandalone() {
        return Boolean.parseBoolean(getEnvVar("pravega_standalone", "true"));
    }

    public static String getUser() {
        return getEnvVar("PRAVEGA_STANDALONE_USER", "admin");
    }

    public static String getPassword() {
        return getEnvVar("PRAVEGA_STANDALONE_PASSWORD", "1111_aaaa");
    }

    public static String getScope() {
        return getEnvVar("PRAVEGA_SCOPE", "workshop-samples");
    }

    public static String getStreamName() {
        return getEnvVar("STREAM_NAME", "workshop-stream");
    }

    public static String getRoutingKeyAttributeName() {
        return getEnvVar("ROUTING_KEY_ATTRIBUTE_NAME", "test");
    }

    public static String getMessage() {
        return getEnvVar("MESSAGE", "This is Nautilus OE team workshop samples.");
    }

    private static String getEnvVar(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }

    public static URI getGatewayURI() {
        return URI.create(getEnvVar("GATEWAY_URI", "http://0.0.0.0:3000/"));
    }

    public static int getTargetRateEventsPerSec() {
        return Integer.parseInt(getEnvVar("PRAVEGA_TARGET_RATE_EVENTS_PER_SEC", "100"));
    }

    public static int getScaleFactor() {
        return Integer.parseInt(getEnvVar("PRAVEGA_SCALE_FACTOR", "2"));
    }

    public static int getMinNumSegments() {
        return Integer.parseInt(getEnvVar("PRAVEGA_MIN_NUM_SEGMENTS", "1"));
    }

    public static int getListenPort() {
        return Integer.parseInt(getEnvVar("LISTEN_PORT", "54672"));
    }

    public static ParameterTool params = null;

    public static void init(String[] args) {
        params = ParameterTool.fromArgs(args);
    }

    public static String getParam(String key) {
        if (params != null && params.has(key)) {
            return params.get(key);
        } else {
            return getDefaultParam(key);
        }
    }

    private static String getDefaultParam(String key) {
        String keyValue = null;
        if (key != null) {
            switch (key) {
                case "pravega_scope":
                    keyValue = "workshop-samples";
                    break;
                case "stream_name":
                    keyValue = "workshop-stream";
                    break;
                case "pravega_controller_uri":
                    keyValue = "tcp://localhost:9090";
                    break;
                case "routing_key_attribute_name":
                    keyValue = "100";
                    break;
                case "pravega_standalone":
                    keyValue = "true";
                    break;
                case "data_file":
                    keyValue = "initial_data_result_nozero.csv";
                    break;
                case "message":
                    keyValue = "To demonstrate Nautilus streams sending a string message";
                    break;
                default:
                    keyValue = null;
            }
        }
        return keyValue;
    }
}