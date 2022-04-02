import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWriteTensorToImageBatchOp(unittest.TestCase):
    def test_writetensortoimagebatchop(self):

        df_data = pd.DataFrame([
            'sphx_glr_plot_scripted_tensor_transforms_001.png'
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'path string')
        
        readImageToTensorBatchOp = ReadImageToTensorBatchOp()\
            .setRootFilePath("https://pytorch.org/vision/stable/_images/")\
        	.setRelativeFilePathCol("path")\
        	.setOutputCol("tensor")
        
        writeTensorToImageBatchOp = WriteTensorToImageBatchOp()\
        			.setRootFilePath("/tmp/write_tensor_to_image")\
        			.setTensorCol("tensor")\
        			.setImageType("png")\
        			.setRelativeFilePathCol("path")
        
        batch_data.link(readImageToTensorBatchOp).link(writeTensorToImageBatchOp).print()
        
        pass