# Copyright 2019 The flink-ai-extended Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================

from pyflink.java_gateway import get_gateway


class TFCONSTANS(object):
    J_CONSTANTS = get_gateway().jvm.com.alibaba.flink.ml.tensorflow.util.TFConstants

    TF_PORT = str(J_CONSTANTS.TF_PORT)
    TF_INFERENCE_EXPORT_PATH = str(J_CONSTANTS.TF_INFERENCE_EXPORT_PATH)
    TF_INFERENCE_INPUT_TENSOR_NAMES = str(J_CONSTANTS.TF_INFERENCE_INPUT_TENSOR_NAMES)
    TF_INFERENCE_OUTPUT_TENSOR_NAMES = str(J_CONSTANTS.TF_INFERENCE_OUTPUT_TENSOR_NAMES)
    TF_INFERENCE_OUTPUT_ROW_FIELDS = str(J_CONSTANTS.TF_INFERENCE_OUTPUT_ROW_FIELDS)
    TF_INFERENCE_BATCH_SIZE = str(J_CONSTANTS.TF_INFERENCE_BATCH_SIZE)
    TF_IS_CHIEF_ALONE = str(J_CONSTANTS.TF_IS_CHIEF_ALONE)
    TF_IS_CHIEF_ROLE = str(J_CONSTANTS.TF_IS_CHIEF_ROLE)
    TENSORBOART_PORT = str(J_CONSTANTS.TENSORBOART_PORT)
    INPUT_TF_EXAMPLE_CONFIG = str(J_CONSTANTS.INPUT_TF_EXAMPLE_CONFIG)
    OUTPUT_TF_EXAMPLE_CONFIG = str(J_CONSTANTS.OUTPUT_TF_EXAMPLE_CONFIG)
