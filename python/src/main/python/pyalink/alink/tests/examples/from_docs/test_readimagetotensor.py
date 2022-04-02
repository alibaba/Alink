import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestReadImageToTensor(unittest.TestCase):
    def test_readimagetotensor(self):

        df_data = pd.DataFrame([
            'sphx_glr_plot_scripted_tensor_transforms_001.png'
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'path string')
        
        ReadImageToTensor()\
            .setRootFilePath("https://pytorch.org/vision/stable/_images/")\
        	.setRelativeFilePathCol("path")\
        	.setOutputCol("tensor")\
            .transform(batch_data)\
            .print()
        
        pass