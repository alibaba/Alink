import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

dtype_str_map = {
    'byte': tf.int8,
    'ubyte': tf.uint8,
    'int': tf.int64,
    'long': tf.int64,
    'float': tf.float32,
    'double': tf.float32,
    'boolean': tf.bool,
    'string': tf.string,
}


def parse_feature_specs(example_config):
    """
    input_ids:int:64-20-3,input_mask:int:64,segment_ids:int:64,label_ids:int:1

    :param example_config:
    :param label_col:
    :return:
    """
    feature_specs = {}
    for feature_config in example_config:
        name = feature_config['name']
        dtype_str = feature_config['dtype']
        shape = feature_config['shape']
        dtype = dtype_str_map[dtype_str]
        feature_specs[name] = tf.FixedLenFeature(shape, dtype)
    return feature_specs


def get_example_parser(feature_specs, label_col: str):
    def parse_example(serial_example: str):
        features = tf.parse_single_example(serial_example, features=feature_specs)
        label = features.pop(label_col)
        return features, label

    return parse_example


def get_feature_placeholders(placeholders_config, export_batch_dim=False, **kwargs):
    placeholders = {}
    for placeholder_config in placeholders_config:
        name = placeholder_config['name']
        dtype_str = placeholder_config['dtype']
        shape = placeholder_config['shape']
        if export_batch_dim:
            shape = [1, *shape]
        dtype = dtype_str_map[dtype_str]
        placeholders[name] = tf.placeholder(dtype, shape=shape, name=name)
    return placeholders


def get_dataset(feature_specs,
                raw_dataset_fn,
                label_col: str,
                batch_size: int,
                num_epochs: int,
                num_parallel_calls: int = 8,
                shuffle_factor: int = 10,
                prefetch_buffer_size: int = None,
                **kwargs):
    dataset = raw_dataset_fn()
    dataset = dataset.map(get_example_parser(feature_specs, label_col),
                          num_parallel_calls=num_parallel_calls)
    if shuffle_factor and shuffle_factor > 0:
        dataset = dataset.shuffle(buffer_size=batch_size * shuffle_factor)
    dataset = dataset.repeat(num_epochs).batch(batch_size)
    if prefetch_buffer_size and prefetch_buffer_size > 0:
        dataset = dataset.prefetch(prefetch_buffer_size)
    return dataset


def get_dataset_fn(**kwargs):
    return lambda: get_dataset(**kwargs)
