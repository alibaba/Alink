import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestReadImageToTensorStreamOp(unittest.TestCase):
    def test_readimagetotensorstreamop(self):

        df_data = pd.DataFrame([
            'sphx_glr_plot_scripted_tensor_transforms_001.png'
        ])
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'path string')
        
        ReadImageToTensorStreamOp()\
            .setRootFilePath("https://pytorch.org/vision/stable/_images/")\
        	.setRelativeFilePathCol("path")\
        	.setOutputCol("tensor")\
            .linkFrom(stream_data)\
            .print()
            
        StreamOperator.execute()
        
        pass