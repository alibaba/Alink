import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

from tensorflow.keras import backend as K
from tensorflow.keras.callbacks import TensorBoard
from tensorflow.keras.layers import Input, GRU, Conv2D, Dropout, Flatten, Dense, Reshape, Concatenate, Add
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import SGD, RMSprop, Adam


#######################################################################################################################
#                                      Start Skip RNN specific layers subsclass                                       #
#                                                                                                                     #
# The SkipRNN layer is implemented as follows:                                                                        #
# - Pre Transformation layer that takes a 'window' size of data and apply couple of reshapes and axis transformation  #
#   in order to simulate the skip RNN that is described in the paper                                                  #
# - Apply a normal GRU RNN on the transformed data. This way not the adjacent data points are connected but rather    #
#   data points that are 'skip' away.                                                                                 #
# - Post Transformation layer that brings back dimensions to its original shape as PreTrans has changed all           #
#   dimensions including Batch Size                                                                                   #
#######################################################################################################################

class PreSkipTrans(tf.keras.layers.Layer):
    def __init__(self, pt, skip, **kwargs):
        #
        # pt:   Number of different RNN cells = (window / skip)
        # skip: Number of points to skip
        #
        self.pt = pt
        self.skip = skip
        super(PreSkipTrans, self).__init__(**kwargs)

    def build(self, input_shape):
        super(PreSkipTrans, self).build(input_shape)

    def call(self, inputs):
        # Get input tensors; in this case it's just one tensor
        x = inputs

        # Get the batchsize which is tf.shape(x)[0] since inputs is either X or C which has the same
        # batchsize as the input to the model
        batchsize = tf.shape(x)[0]

        # Get the shape of the input data
        input_shape = K.int_shape(x)

        # Create output data by taking a 'window' size from the end of input (:-self.pt * self.skip)
        output = x[:, -self.pt * self.skip:, :]

        # Reshape the output tensor by:
        # - Changing first dimension (batchsize) from None to the current batchsize
        # - Splitting second dimension into 2 dimensions
        output = tf.reshape(output, [batchsize, self.pt, self.skip, input_shape[2]])

        # Permutate axis 1 and axis 2
        output = tf.transpose(output, perm=[0, 2, 1, 3])

        # Reshape by merging axis 0 and 1 now hence changing the batch size
        # to be equal to current batchsize * skip.
        # This way the time dimension will only contain 'pt' entries which are
        # just values that were originally 'skip' apart from each other => hence skip RNN ready
        output = tf.reshape(output, [batchsize * self.skip, self.pt, input_shape[2]])

        # Adjust the output shape by setting back the batch size dimension to None
        output_shape = tf.TensorShape([None]).concatenate(output.get_shape()[1:])

        return output

    def compute_output_shape(self, input_shape):
        # Since the batch size is None and dimension on axis=2 has not changed,
        # all we need to do is set shape[1] = pt in order to compute the output shape
        shape = tf.TensorShape(input_shape).as_list()
        shape[1] = self.pt

        return tf.TensorShape(shape)

    def get_config(self):
        config = {'pt': self.pt, 'skip': self.skip}
        base_config = super(PreSkipTrans, self).get_config()

        return dict(list(base_config.items()) + list(config.items()))

    @classmethod
    def from_config(cls, config):
        return cls(**config)


class PostSkipTrans(tf.keras.layers.Layer):
    def __init__(self, skip, **kwargs):
        #
        # skip: Number of points to skip
        #
        self.skip = skip
        super(PostSkipTrans, self).__init__(**kwargs)

    def build(self, input_shape):
        super(PostSkipTrans, self).build(input_shape)

    def call(self, inputs):
        # Get input tensors
        # - First one is the output of the SkipRNN layer which we will operate on
        # - The second is the oiriginal model input tensor which we will use to get
        #   the original batchsize
        x, original_model_input = inputs

        # Get the batchsize which is tf.shape(original_model_input)[0]
        batchsize = tf.shape(original_model_input)[0]

        # Get the shape of the input data
        input_shape = K.int_shape(x)

        # Split the batch size into the original batch size before PreTrans and 'Skip'
        output = tf.reshape(x, [batchsize, self.skip, input_shape[1]])

        # Merge the 'skip' with axis=1
        output = tf.reshape(output, [batchsize, self.skip * input_shape[1]])

        # Adjust the output shape by setting back the batch size dimension to None
        output_shape = tf.TensorShape([None]).concatenate(output.get_shape()[1:])

        return output

    def compute_output_shape(self, input_shape):
        # Adjust shape[1] to be equal to shape[1] * skip in order for the
        # shape to reflect the transformation that was done
        shape = tf.TensorShape(input_shape).as_list()
        shape[1] = self.skip * shape[1]

        return tf.TransformShape(shape)

    def get_config(self):
        config = {'skip': self.skip}
        base_config = super(PostSkipTrans, self).get_config()

        return dict(list(base_config.items()) + list(config.items()))

    @classmethod
    def from_config(cls, config):
        return cls(**config)


#######################################################################################################################
#                                      End Skip RNN specific layers subsclass                                         #
#######################################################################################################################


#######################################################################################################################
#                                      Start AR specific layers subsclass                                             #
#                                                                                                                     #
# The AR layer is implemented as follows:                                                                             #
# - Pre Transformation layer that takes a 'highway' size of data and apply a reshape and axis transformation          #
# - Flatten the output and pass it through a Dense layer with one output                                              #
# - Post Transformation layer that bring back dimensions to its original shape                                        #                                                     #
#######################################################################################################################

class PreARTrans(tf.keras.layers.Layer):
    def __init__(self, hw, **kwargs):
        #
        # hw: Highway = Number of timeseries values to consider for the linear layer (AR layer)
        #
        self.hw = hw
        super(PreARTrans, self).__init__(**kwargs)

    def build(self, input_shape):
        super(PreARTrans, self).build(input_shape)

    def call(self, inputs):
        # Get input tensors; in this case it's just one tensor: X = the input to the model
        x = inputs

        # Get the batchsize which is tf.shape(x)[0]
        batchsize = tf.shape(x)[0]

        # Get the shape of the input data
        input_shape = K.int_shape(x)

        # Select only 'highway' length of input to create output
        output = x[:, -self.hw:, :]

        # Permute axis 1 and 2. axis=2 is the the dimension having different time-series
        # This dimension should be equal to 'm' which is the number of time-series.
        output = tf.transpose(output, perm=[0, 2, 1])

        # Merge axis 0 and 1 in order to change the batch size
        output = tf.reshape(output, [batchsize * input_shape[2], self.hw])

        # Adjust the output shape by setting back the batch size dimension to None
        output_shape = tf.TensorShape([None]).concatenate(output.get_shape()[1:])

        return output

    def compute_output_shape(self, input_shape):
        # Set the shape of axis=1 to be hw since the batchsize is NULL
        shape = tf.TensorShape(input_shape).as_list()
        shape[1] = self.hw

        return tf.TensorShape(shape)

    def get_config(self):
        config = {'hw': self.hw}
        base_config = super(PreARTrans, self).get_config()

        return dict(list(base_config.items()) + list(config.items()))

    @classmethod
    def from_config(cls, config):
        return cls(**config)


class PostARTrans(tf.keras.layers.Layer):
    def __init__(self, m, **kwargs):
        #
        # m: Number of timeseries
        #
        self.m = m
        super(PostARTrans, self).__init__(**kwargs)

    def build(self, input_shape):
        super(PostARTrans, self).build(input_shape)

    def call(self, inputs):
        # Get input tensors
        # - First one is the output of the Dense(1) layer which we will operate on
        # - The second is the oiriginal model input tensor which we will use to get
        #   the original batchsize
        x, original_model_input = inputs

        # Get the batchsize which is tf.shape(original_model_input)[0]
        batchsize = tf.shape(original_model_input)[0]

        # Get the shape of the input data
        input_shape = K.int_shape(x)

        # Reshape the output to have the batch size equal to the original batchsize before PreARTrans
        # and the second dimension as the number of timeseries
        output = tf.reshape(x, [batchsize, self.m])

        # Adjust the output shape by setting back the batch size dimension to None
        output_shape = tf.TensorShape([None]).concatenate(output.get_shape()[1:])

        return output

    def compute_output_shape(self, input_shape):
        # Adjust shape[1] to be equal 'm'
        shape = tf.TensorShape(input_shape).as_list()
        shape[1] = self.m

        return tf.TensorShape(shape)

    def get_config(self):
        config = {'m': self.m}
        base_config = super(PostARTrans, self).get_config()

        return dict(list(base_config.items()) + list(config.items()))

    @classmethod
    def from_config(cls, config):
        return cls(**config)


#######################################################################################################################
#                                      End AR specific layers subsclass                                               #
#######################################################################################################################

#######################################################################################################################
#                                                 Model Start                                                         #
#                                                                                                                     #
# The model, as per the paper has the following layers:                                                               #
# - CNN                                                                                                               #
# - GRU                                                                                                               #
# - SkipGRU                                                                                                           #
# - AR                                                                                                                #
#######################################################################################################################

def LSTNetModel(init, input_name, input_shape):
    # m is the number of time-series
    m = input_shape[2]

    # Get tensor shape except batchsize
    tensor_shape = input_shape[1:]

    if K.image_data_format() == 'channels_last':
        ch_axis = 3
    else:
        ch_axis = 1

    X = Input(shape=tensor_shape, name=input_name)

    # CNN
    if init.CNNFilters > 0 and init.CNNKernel > 0:
        # Add an extra dimension of size 1 which is the channel dimension in Conv2D
        C = Reshape((input_shape[1], input_shape[2], 1))(X)

        # Apply a Conv2D that will transform it into data of dimensions (batchsize, time, 1, NumofFilters)
        C = Conv2D(filters=init.CNNFilters, kernel_size=(init.CNNKernel, m), kernel_initializer=init.initialiser)(C)
        C = Dropout(init.dropout)(C)

        # Adjust data dimensions by removing axis=2 which is always equal to 1
        c_shape = K.int_shape(C)
        C = Reshape((c_shape[1], c_shape[3]))(C)
    else:
        # If configured not to apply CNN, copy the input
        C = X

    # GRU
    # Apply a GRU layer (with activation set to 'relu' as per the paper) and take the returned states as result
    _, R = GRU(init.GRUUnits, activation="relu", return_sequences=False, return_state=True)(C)
    R = Dropout(init.dropout)(R)

    # SkipGRU
    if init.skip > 0:
        # Calculate the number of values to use which is equal to the window divided by how many time values to skip
        pt = int(init.window / init.skip)

        S = PreSkipTrans(pt, int((init.window - init.CNNKernel + 1) / pt))(C)
        _, S = GRU(init.SkipGRUUnits, activation="relu", return_sequences=False, return_state=True)(S)
        S = PostSkipTrans(int((init.window - init.CNNKernel + 1) / pt))([S, X])

        # Concatenate the outputs of GRU and SkipGRU
        R = Concatenate(axis=1)([R, S])

    # Dense layer
    Y = Flatten()(R)
    Y = Dense(m)(Y)

    # AR
    if init.highway > 0:
        Z = PreARTrans(init.highway)(X)
        Z = Flatten()(Z)
        Z = Dense(1)(Z)
        Z = PostARTrans(m)([Z, X])

        # Generate output as the summation of the Dense layer output and the AR one
        Y = Add()([Y, Z])

    # Generate Model
    model = Model(inputs=X, outputs=Y)

    return model


#######################################################################################################################
#                                                 Model End                                                           #
#######################################################################################################################

#######################################################################################################################
#                                                 Model Utilities                                                     #
#                                                                                                                     #
# Below is a collection of functions:                                                                                 #
# - rse:         A metrics function that calculates the root square error                                             #
# - corr:        A metrics function that calculates the correlation                                                   #
#######################################################################################################################
def rse(y_true, y_pred):
    #
    # The formula is:
    #           K.sqrt(K.sum(K.square(y_true - y_pred)))
    #    RSE = -----------------------------------------------
    #           K.sqrt(K.sum(K.square(y_true_mean - y_true)))
    #
    #           K.sqrt(K.sum(K.square(y_true - y_pred))/(N-1))
    #        = ----------------------------------------------------
    #           K.sqrt(K.sum(K.square(y_true_mean - y_true)/(N-1)))
    #
    #
    #           K.sqrt(K.mean(K.square(y_true - y_pred)))
    #        = ------------------------------------------
    #           K.std(y_true)
    #
    num = K.sqrt(K.mean(K.square(y_true - y_pred), axis=None))
    den = K.std(y_true, axis=None)

    return num / den


def corr(y_true, y_pred):
    #
    # This function calculates the correlation between the true and the predicted outputs
    #
    num1 = y_true - K.mean(y_true, axis=0)
    num2 = y_pred - K.mean(y_pred, axis=0)

    num = K.mean(num1 * num2, axis=0)
    den = K.std(y_true, axis=0) * K.std(y_pred, axis=0)

    return K.mean(num / den)


#
# A function that compiles 'model' after setting the appropriate:
# - optimiser function passed via init
# - learning rate passed via init
# - loss function also set in init
# - metrics
#
def ModelCompile(model, init):
    # Select the appropriate optimiser and set the learning rate from input values (arguments)
    if init.optimiser == "SGD":
        opt = SGD(lr=init.lr, momentum=0.0, decay=0.0, nesterov=False)
    elif init.optimiser == "RMSprop":
        opt = RMSprop(lr=init.lr, rho=0.9, epsilon=None, decay=0.0)
    else:  # Adam
        opt = Adam(lr=init.lr, beta_1=0.9, beta_2=0.999, epsilon=None, decay=0.0, amsgrad=False)

    # Compile using the previously defined metrics
    model.compile(optimizer=opt, loss=init.loss, metrics=[rse, corr])

    # Launch Tensorboard if selected in arguments
    if init.tensorboard != None:
        tensorboard = TensorBoard(log_dir=init.tensorboard)
    else:
        tensorboard = None

    return tensorboard
