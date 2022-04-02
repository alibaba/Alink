import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWriteTensorToImageStreamOp(unittest.TestCase):
    def test_writetensortoimagestreamop(self):

        df_data = pd.DataFrame([
            'sphx_glr_plot_scripted_tensor_transforms_001.png'
        ])
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'path string')
        
        readImageToTensorStreamOp = ReadImageToTensorStreamOp()\
            .setRootFilePath("https://pytorch.org/vision/stable/_images/")\
        	.setRelativeFilePathCol("path")\
        	.setOutputCol("tensor")
        
        writeTensorToImageStreamOp = WriteTensorToImageStreamOp()\
        			.setRootFilePath("/tmp/write_tensor_to_image")\
        			.setTensorCol("tensor")\
        			.setImageType("png")\
        			.setRelativeFilePathCol("path")
        
        stream_data.link(readImageToTensorStreamOp).link(writeTensorToImageStreamOp).print()
        
        StreamOperator.execute()
        pass