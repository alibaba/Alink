import os
from pathlib import Path

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1
    tf.disable_v2_behavior()

from akdl.engine.inputs import parse_feature_specs, get_dataset_fn
from akdl.models.tf.tft import tft_main
from akdl.models.tf.tft.data_formatters.volatility import VolatilityFormatter
from akdl.models.tf.tft.expt_settings.configs import ExperimentConfig
from akdl.models.tf.tft.libs import utils
from akdl.models.tf.tft.libs.tft_model import TemporalFusionTransformer
from akdl.models.tf.tft.script_train_fixed_params import main
from akdl.runner.config import TrainTaskConfig


def test_tft(tmp_path):
    print(tmp_path)
    name, output_folder, use_tensorflow_with_gpu = ('volatility', 'volatility_output', False)
    config = ExperimentConfig(name, output_folder)
    formatter = config.make_data_formatter()

    # Customise inputs to main() for new datasets.
    main(
        expt_name=name,
        use_gpu=use_tensorflow_with_gpu,
        model_folder=os.path.join(config.model_folder, "fixed"),
        data_csv_path=config.data_csv_path,
        data_formatter=formatter,
        use_testing_mode=True)  # Change to `false` to use original default params


def test_tft_dataset(tmp_path):
    print(tmp_path)
    tfrecords_path = "/apsarapangu/disk1/jiqi_workspace_DO_NOT_DELETE/code/akdl/akdl_open/akdl/tests/models/tf/tft/tft.tfrecords"
    from akdl.runner.tf_helper import preview_tfrecords_file
    preview_tfrecords_file(tfrecords_path)

    formatter = VolatilityFormatter()

    params = {
    }
    model_params = formatter.get_default_model_params()
    fixed_params = formatter.get_experiment_params()
    params.update(model_params)
    params.update(fixed_params)
    params.update({
        'input_size': 8,
        'output_size': 1,
        'category_counts': [
            7,
            31,
            53,
            12,
            4
        ],
        'input_obs_loc': [
            0
        ],
        'static_input_loc': [
            7
        ],
        'known_regular_inputs': [
            2
        ],
        'known_categorical_inputs': [
            0,
            1,
            2,
            3,
            4
        ]
    })
    params.update({
        'model_folder': "./my_data"
    })

    default_keras_session = tf.keras.backend.get_session()
    tf_config = utils.get_default_tensorflow_config(tf_device="cpu")
    tf.reset_default_graph()
    with tf.Graph().as_default(), tf.Session(config=tf_config) as sess:
        tf.keras.backend.set_session(sess)

        example_config = [
            {
                'name': 'inputs',
                'dtype': 'float',
                'shape': [257, 8],
            },
            {
                'name': 'label',
                'dtype': 'float',
                'shape': [5, 1],
            }
        ]
        label_col = 'label'
        feature_specs = parse_feature_specs(example_config)
        dataset_fn = get_dataset_fn(
            raw_dataset_fn=lambda: tf.data.TFRecordDataset(tfrecords_path),
            feature_specs=feature_specs,
            label_col=label_col,
            batch_size=32,
            num_epochs=10,
        )
        model = TemporalFusionTransformer(params, use_cudnn=False)
        sess.run(tf.global_variables_initializer())
        model.fit_dataset(dataset_fn())
        tf.keras.backend.set_session(default_keras_session)
        model.model.save("./my_model", save_format='tf')
        print("Here", flush=True)


def test_from_raw_time_series(tmp_path):
    tmp_path = Path("/tmp/work_dir_0_2672166819404446880")
    print(tmp_path)
    tfrecords_path = str(tmp_path / "dataset.tfrecords")

    user_params = {
        "ALINK:gpu_devices": "[]", "tensorCol": "inputs", "batch_size": "128",
        "fixed_params": "{\"total_time_steps\":\"257\",\"num_encoder_steps\":\"252\",\"early_stopping_patience\":\"5\",\"multiprocessing_workers\":\"5\",\"num_epochs\":\"1\"}",
        "model_params": "{\"dropout_rate\":\"0.3\",\"hidden_layer_size\":\"160\",\"learning_rate\":\"0.001\",\"num_heads\":\"1\",\"stack_size\":\"1\",\"minibatch_size\":\"128\",\"max_gradient_norm\":\"0.01\"}",
        "labelCol": "labels", "num_epochs": "1",
        "column_definitions": "[[\"log_vol\",\"REAL_VALUED\",\"TARGET\"],[\"date\",\"DATE\",\"TIME\"],[\"Symbol\",\"CATEGORICAL\",\"ID\"],[\"open_to_close\",\"REAL_VALUED\",\"OBSERVED_INPUT\"],[\"days_from_start\",\"REAL_VALUED\",\"KNOWN_INPUT\"],[\"day_of_week\",\"CATEGORICAL\",\"KNOWN_INPUT\"],[\"day_of_month\",\"CATEGORICAL\",\"KNOWN_INPUT\"],[\"week_of_year\",\"CATEGORICAL\",\"KNOWN_INPUT\"],[\"month\",\"CATEGORICAL\",\"KNOWN_INPUT\"],[\"Region\",\"CATEGORICAL\",\"STATIC_INPUT\"]]",
        "ALINK:checkpoint_path": "/tmp/tft_model",
        "ALINK:bc_1": "/tmp/work_dir_0_6401616608203947302/bc_data_1"
    }

    args: TrainTaskConfig = TrainTaskConfig(
        dataset_file=tfrecords_path,
        tf_context=None,
        num_workers=1, cluster=None, task_type='chief', task_index=0,
        work_dir=str(tmp_path),
        dataset=None, dataset_length=10,
        saved_model_dir=str(tmp_path / 'saved_model_dir'),
        user_params=user_params)
    tft_main.main(args)
