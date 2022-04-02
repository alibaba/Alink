import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestExtractMfccFeature(unittest.TestCase):
    def test_extractmfccfeature(self):

        DATA_DIR = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/audio"
        SAMPLE_RATE = 16000
        
        df = pd.DataFrame([
            "246.wav",
            "247.wav"
        ])
        
        source = BatchOperator.fromDataframe(df, "wav_file_path string") \
                .link(ReadAudioToTensorBatchOp()
                        .setRootFilePath(DATA_DIR)
                        .setSampleRate(SAMPLE_RATE)
                        .setRelativeFilePathCol("wav_file_path")
                        .setDuration(2.)
                        .setOutputCol("tensor")
                )
        
        mfcc = ExtractMfccFeature() \
                .setSelectedCol("tensor") \
                .setSampleRate(SAMPLE_RATE) \
                .setWindowTime(0.128) \
                .setHopTime(0.032) \
                .setNumMfcc(26) \
                .setOutputCol("mfcc")
        
        mfcc.transform(source).print()
        pass