from typing import Dict
import math

import tensorflow as tf
from tensorflow.python.keras.activations import softplus

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

from tensorflow.keras import backend as K
from tensorflow.keras.callbacks import TensorBoard
from tensorflow.keras.layers import Input, GRU, Conv2D, Dropout, Flatten, Dense, Reshape, Concatenate, Add, Layer
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import SGD, RMSprop, Adam
from tensorflow.python.keras.layers import LSTM


class GaussianLayer(Layer):
    def __init__(self, output_dim, **kwargs):
        self.output_dim = output_dim
        self.dense_mu = None
        self.dense_pre_sigma = None
        super(GaussianLayer, self).__init__(**kwargs)

    def build(self, input_shape):
        self.dense_mu = Dense(self.output_dim)
        self.dense_pre_sigma = Dense(self.output_dim)
        super(GaussianLayer, self).build(input_shape)

    def call(self, inputs, **kwargs):
        mu = self.dense_mu(inputs)
        pre_sigma = self.dense_pre_sigma(inputs)
        sigma = softplus(pre_sigma)
        return mu, sigma

    def get_config(self):
        config = super(GaussianLayer, self).get_config().copy()
        config.update({
            'output_dim': self.output_dim
        })
        return config


def DeepARModel(args: Dict):
    input_name = args.get('input_name')
    input_shape = args.get('input_shape')
    lstm_units = args.get('lstm_units')
    dense_units = args.get('dense_units')

    inputs = Input(shape=input_shape, name=input_name)
    x = LSTM(lstm_units, return_sequences=True)(inputs)
    x = Dense(dense_units, activation='relu')(x)
    mu, sigma = GaussianLayer(1, name='gaussian')(x)
    outputs = tf.concat((mu, sigma), axis=2, name='output')
    return Model(inputs, outputs)


def gaussian_loss(y_true, y_pred):
    mu = y_pred[:, :, 0]
    sigma = y_pred[:, :, 1]
    tf.logging.warn("y_true = {}, mu = {}, sigma = {}".format(y_true, mu, sigma))
    log_likelihood = -0.5 * ((y_true - mu) ** 2 / sigma ** 2) - tf.math.log(sigma) - tf.math.log(
        tf.math.sqrt(2 * math.pi))
    loss = -tf.reduce_mean(log_likelihood)
    return loss
