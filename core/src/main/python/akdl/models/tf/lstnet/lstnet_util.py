import argparse


class LSTNetInit(object):
    #
    # This class contains all initialisation information that are passed as arguments.
    #
    #    data:            Location of the data file
    #    window:          Number of time values to consider in each input X
    #                        Default: 24*7
    #    horizon:         How far is the predicted value Y. It is horizon values away from the last value of X (into the future)
    #                        Default: 12
    #    CNNFilters:      Number of output filters in the CNN layer
    #                        Default: 100
    #                        If set to 0, the CNN layer will be omitted
    #    CNNKernel:       CNN filter size that will be (CNNKernel, number of multivariate timeseries)
    #                        Default: 6
    #                        If set to 0, the CNN layer will be omitted
    #    GRUUnits:        Number of hidden states in the GRU layer
    #                        Default: 100
    #    SkipGRUUnits:    Number of hidden states in the SkipGRU layer
    #                        Default: 5
    #    skip:            Number of timeseries to skip. 0 => do not add Skip GRU layer
    #                        Default: 24
    #                        If set to 0, the SkipGRU layer will be omitted
    #    dropout:         Dropout frequency
    #                        Default: 0.2
    #    normalise:       Type of normalisation:
    #                     - 0: No normalisation
    #                     - 1: Normalise all timeseries together
    #                     - 2: Normalise each timeseries alone
    #                        Default: 2
    #    batchsize:       Training batch size
    #                        Default: 128
    #    epochs:          Number of training epochs
    #                        Default: 100
    #    initialiser:     The weights initialiser to use.
    #                        Default: glorot_uniform
    #    trainpercent:    How much percentage of the given data to use for training.
    #                        Default: 0.6 (60%)
    #    validpercent:    How much percentage of the given data to use for validation.
    #                        Default: 0.2 (20%)
    #                     The remaining (1 - trainpercent -validpercent) shall be the amount of test data
    #    highway:         Number of timeseries values to consider for the linear layer (AR layer)
    #                        Default: 24
    #                        If set to 0, the AR layer will be omitted
    #    train:           Whether to train the model or not
    #                        Default: True
    #    validate:        Whether to validate the model against the validation data
    #                        Default: True
    #                     If set and train is set, validation will be done while training.
    #    evaltest:        Evaluate the model using testing data
    #                        Default: False
    #    save:            Location and Name of the file to save the model in as follows:
    #                           Model in "save.json"
    #                           Weights in "save.h5"
    #                        Default: None
    #                     This location is also used to save results and history in, as follows:
    #                           Results in "save.txt" if --saveresults is passed
    #                           History in "save_history.csv" if --savehistory is passed
    #    saveresults:     Save results as described in 'save' above.
    #                     This has no effect if --save is not set
    #                        Default: True
    #    savehistory:     Save training / validation history as described in 'save' above.
    #                     This has no effect if --save is not set
    #                        Default: False
    #    load:            Location and Name of the file to load a pretrained model from as follows:
    #                           Model in "load.json"
    #                           Weights in "load.h5"
    #                        Default: None
    #    loss:            The loss function to use for optimisation.
    #                        Default: mean_absolute_error
    #    lr:              Learning rate
    #                        Default: 0.001
    #    optimizer:       The optimiser to use
    #                        Default: Adam
    #    test:            Evaluate the model on the test data
    #                        Default: False
    #    tensorboard:     Set to the folder where to put tensorboard file
    #                        Default: None (no tensorboard callback)
    #    predict:         Predict timeseries using the trained model.
    #                     It takes one of the following values:
    #                     - trainingdata   => predict the training data only
    #                     - validationdata => predict the validation data only
    #                     - testingdata    => predict the testing data only
    #                     - all            => all of the above
    #                     - None           => none of the above
    #                        Default: None
    #    plot:            Generate plots
    #                        Default: False
    #    series_to_plot:  The number of the series that you wish to plot. The value must be less than the number of series available
    #                        Default: 0
    #    autocorrelation: The number of the random series that you wish to plot their autocorrelation. The value must be less or equal to the number of series available
    #                        Default: None
    #    save_plot:       Location and Name of the file to save the plotted images to as follows:
    #                           Autocorrelation in "save_plot_autocorrelation.png"
    #                           Training results in "save_plot_training.png"
    #                           Prediction in "save_plot_prediction.png"
    #                        Default: None
    #    log:             Whether to generate logging
    #                        Default: True
    #    debuglevel:      Logging debuglevel.
    #                     It takes one of the following values:
    #                     - 10 => DEBUG
    #                     - 20 => INFO
    #                     - 30 => WARNING
    #                     - 40 => ERROR
    #                     - 50 => CRITICAL
    #                        Default: 20
    #    logfilename:     Filename where logging will be written.
    #                        Default: log/lstnet
    #
    def __init__(self, args, args_is_dictionary=False):
        if args_is_dictionary is True:
            self.data = args["data"]
            self.window = args["window"]
            self.horizon = args["horizon"]
            self.CNNFilters = args["CNNFilters"]
            self.CNNKernel = args["CNNKernel"]
            self.GRUUnits = args["GRUUnits"]
            self.SkipGRUUnits = args["SkipGRUUnits"]
            self.skip = args["skip"]
            self.dropout = args["dropout"]
            self.normalise = args["normalize"]
            self.highway = args["highway"]
            self.batchsize = args["batchsize"]
            self.epochs = args["epochs"]
            self.initialiser = args["initializer"]
            self.trainpercent = args["trainpercent"]
            self.validpercent = args["validpercent"]
            self.highway = args["highway"]
            self.train = not args["no_train"]
            self.validate = not args["no_validation"]
            self.save = args["save"]
            self.saveresults = not args["no_saveresults"]
            self.savehistory = args["savehistory"]
            self.load = args["load"]
            self.loss = args["loss"]
            self.lr = args["lr"]
            self.optimiser = args["optimizer"]
            self.evaltest = args["test"]
            self.tensorboard = args["tensorboard"]
            self.plot = args["plot"]
            self.predict = args["predict"]
            self.series_to_plot = args["series_to_plot"]
            self.autocorrelation = args["autocorrelation"]
            self.save_plot = args["save_plot"]
            self.log = not args["no_log"]
            self.debuglevel = args["debuglevel"]
            self.logfilename = args["logfilename"]
        else:
            self.data = args.data
            self.window = args.window
            self.horizon = args.horizon
            self.CNNFilters = args.CNNFilters
            self.CNNKernel = args.CNNKernel
            self.GRUUnits = args.GRUUnits
            self.SkipGRUUnits = args.SkipGRUUnits
            self.skip = args.skip
            self.dropout = args.dropout
            self.normalise = args.normalize
            self.highway = args.highway
            self.batchsize = args.batchsize
            self.epochs = args.epochs
            self.initialiser = args.initializer
            self.trainpercent = args.trainpercent
            self.validpercent = args.validpercent
            self.highway = args.highway
            self.train = not args.no_train
            self.validate = not args.no_validation
            self.save = args.save
            self.saveresults = not args.no_saveresults
            self.savehistory = args.savehistory
            self.load = args.load
            self.loss = args.loss
            self.lr = args.lr
            self.optimiser = args.optimizer
            self.evaltest = args.test
            self.tensorboard = args.tensorboard
            self.plot = args.plot
            self.predict = args.predict
            self.series_to_plot = args.series_to_plot
            self.autocorrelation = args.autocorrelation
            self.save_plot = args.save_plot
            self.log = not args.no_log
            self.debuglevel = args.debuglevel
            self.logfilename = args.logfilename

    def dump(self):
        from __main__ import logger_name
        import logging
        log = logging.getLogger(logger_name)

        log.debug("Data: %s", self.data)
        log.debug("Window: %d", self.window)
        log.debug("Horizon: %d", self.horizon)
        log.debug("CNN Filters: %d", self.CNNFilters)
        log.debug("CNN Kernel: %d", self.CNNKernel)
        log.debug("GRU Units: %d", self.GRUUnits)
        log.debug("Skip GRU Units: %d", self.SkipGRUUnits)
        log.debug("Skip: %d", self.skip)
        log.debug("Dropout: %f", self.dropout)
        log.debug("Normalise: %d", self.normalise)
        log.debug("Highway: %d", self.highway)
        log.debug("Batch size: %d", self.batchsize)
        log.debug("Epochs: %d", self.epochs)
        log.debug("Learning rate: %s", str(self.lr))
        log.debug("Initialiser: %s", self.initialiser)
        log.debug("Optimiser: %s", self.optimiser)
        log.debug("Loss function to use: %s", self.loss)
        log.debug("Fraction of data to be used for training: %.2f", self.trainpercent)
        log.debug("Fraction of data to be used for validation: %.2f", self.validpercent)
        log.debug("Train model: %s", self.train)
        log.debug("Validate model: %s", self.validate)
        log.debug("Test model: %s", self.evaltest)
        log.debug("Save model location: %s", self.save)
        log.debug("Save Results: %s", self.saveresults)
        log.debug("Save History: %s", self.savehistory)
        log.debug("Load Model from: %s", self.load)
        log.debug("TensorBoard: %s", self.tensorboard)
        log.debug("Plot: %s", self.plot)
        log.debug("Predict: %s", self.predict)
        log.debug("Series to plot: %s", self.series_to_plot)
        log.debug("Save plot: %s", self.save_plot)
        log.debug("Create log: %s", self.log)
        log.debug("Debug level: %d", self.debuglevel)
        log.debug("Logfile: %s", self.logfilename)


def GetArguments(args=None):
    # Creating the argument parser
    parser = argparse.ArgumentParser(description='LSTNet Model')

    parser.add_argument('--data', type=str, required=False, help='Location of the data file. Required!!')
    parser.add_argument('--window', type=int, default=24 * 7, help='Window size. Default=24*7')
    parser.add_argument('--horizon', type=int, default=12, help='Horizon width. Default=12')
    parser.add_argument('--CNNFilters', type=int, default=100,
                        help='Number of CNN layer filters. Default=100. If set to 0, the CNN layer will be omitted')
    parser.add_argument('--CNNKernel', type=int, default=6,
                        help='Size of the CNN filters. Default=6. If set to 0, the CNN layer will be omitted')
    parser.add_argument('--GRUUnits', type=int, default=100, help='Number of GRU hidden units. Default=100')
    parser.add_argument('--SkipGRUUnits', type=int, default=5,
                        help='Number of hidden units in the Skip GRU layer. Default=5')
    parser.add_argument('--skip', type=int, default=24,
                        help='Size of the window to skip in the Skip GRU layer. Default=24. If set to 0, the SkipGRU layer will be omitted')
    parser.add_argument('--dropout', type=float, default=0.2,
                        help='Dropout to be applied to layers. 0 means no dropout. Default=0.2')
    parser.add_argument('--normalize', type=int, default=2,
                        help='0 = do not normalise, 1 = use same normalisation for all timeseries, 2 = normalise each timeseries independently. Default=2')
    parser.add_argument('--highway', type=int, default=24,
                        help='The window size of the highway component. Default=24. If set to 0, the AR layer will be omitted')
    parser.add_argument('--lr', type=float, default=0.001, help='Learning rate. Default=0.001')
    parser.add_argument('--batchsize', type=int, default=128, help='Training batchsize. Default=128')
    parser.add_argument('--epochs', type=int, default=100, help='Number of epochs to run for training. Default=100')
    parser.add_argument('--initializer', type=str, default="glorot_uniform",
                        help='Weights initialiser to use. Default=glorot_uniform')
    parser.add_argument('--loss', type=str, default="mean_absolute_error",
                        help='Loss function to use. Default=mean_absolute_error')
    parser.add_argument('--optimizer', type=str, default="Adam", help='Optimisation function to use. Default=Adam')
    parser.add_argument('--trainpercent', type=float, default=0.6,
                        help='Percentage of data to be used for training. Default=0.6')
    parser.add_argument('--validpercent', type=float, default=0.2,
                        help='Percentage of data to be used for validation. Default=0.2')
    parser.add_argument('--save', type=str, default=None,
                        help='Filename initial to save the model and the results in. Default=None')
    parser.add_argument('--load', type=str, default=None,
                        help='Filename initial of the saved model to be loaded (model and weights). Default=None')
    parser.add_argument('--tensorboard', type=str, default=None,
                        help='Location of the tensorboard folder. If not set, tensorboard will not be launched. Default=None i.e. no tensorboard callback')
    parser.add_argument('--predict', type=str, choices=['trainingdata', 'validationdata', 'testingdata', 'all', None],
                        default=None,
                        help='Predict timesseries. Default None')
    parser.add_argument('--series-to-plot', type=str, default='0',
                        help='Series to plot. Default 0 (i.e. plot the first timeseries)')
    parser.add_argument('--autocorrelation', type=str, default=None,
                        help='Plot an autocorrelation of the input data. Format --autocorrelation=i,j,k which means to plot an autocorrelation of timeseries i from timeslot j to timeslot k')
    parser.add_argument('--save-plot', type=str, default=None,
                        help='Filename initial to save the plots to in PNG format. Default=None')

    parser.add_argument('--no-train', action='store_true', help='Do not train model.')
    parser.add_argument('--no-validation', action='store_true',
                        help='Do not validate model. When not set and no-train is not set, data will be validated while training')
    parser.add_argument('--test', action='store_true', help='Test model.')
    parser.add_argument('--no-saveresults', action='store_true', help='Do not save training / validation results.')
    parser.add_argument('--savehistory', action='store_true', help='Save training / validation history.')
    parser.add_argument('--plot', action='store_true', help='Generate plots.')

    parser.add_argument('--no-log', action='store_true',
                        help='Do not create log files. Only error and critical messages will appear on the console.')
    parser.add_argument('--debuglevel', type=int, choices=[10, 20, 30, 40, 50], default=20,
                        help='Logging debug level. Default 20 (INFO)')
    parser.add_argument('--logfilename', type=str, default="log/lstnet",
                        help="Filename where logging will be written. Default: log/lstnet")

    args = parser.parse_args(args=args)

    return args
