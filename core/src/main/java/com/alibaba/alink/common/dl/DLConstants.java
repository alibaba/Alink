package com.alibaba.alink.common.dl;

public class DLConstants {
    public static final String PYTHON_ENV = "ALINK:python_env";
    public static final String ENTRY_SCRIPT = "ALINK:script";
    public static final String ENTRY_FUNC = "ALINK:entry_func";
    public static final String WORK_DIR = "ALINK:work_dir";
    public static final String USER_DEFINED_PARAMS = "ALINK:user_defined_params";
    public static final String NUM_WORKERS = "ALINK:num_workers";
    public static final String NUM_PSS = "ALINK:num_pss";
    public static final String BC_NAME_PREFIX = "ALINK:bc_";
    public static final String BC_NAME_DOWNLOAD_PATHS = "ALINK:bc_download_path";
    public static final String BC_NAME_TENSOR_SHAPES = "ALINK:bc_tensor_shapes";
	public static final String BC_NAME_TENSOR_TYPES = "ALINK:bc_tensor_types";
    public static final String EXTERNAL_FILE_CONFIG_JSON = "ALINK:external_file_config_json";
	public static final String IP_PORT_BC_NAME = "bcIpPorts";
    public static final String REMOVE_CHECKPOINT_BEFORE_TRAINING = "ALINK:remove_checkpoint_before_training";
    public static final String INPUT_FORMAT = "ALINK:input_format";
    public static final String INTRA_OP_PARALLELISM = "ALINK:intra_op_parallelism";
}
