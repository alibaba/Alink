package com.alibaba.alink.operator.batch.audio;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ReadAudioToTensorBatchOpTest extends AlinkTestBase {

    @Test
    public void testReadAudioToTensorOp() throws Exception {
        String DATA_DIR = "http://alink-audio-data.oss-cn-hangzhou-zmf.aliyuncs.com/casia/wangzhe/happy";
        String[] allFiles = {"246.wav"};
        int sampleRate = 16000;
        List<Row> rows = new MemSourceBatchOp(allFiles, "wav_file_path")
                .link(new ReadAudioToTensorBatchOp()
                        .setRootFilePath(DATA_DIR)
                        .setSampleRate(sampleRate)
                        .setRelativeFilePathCol("wav_file_path")
                        .setDuration(2)
                        .setOutputCol("tensor")
                ).collect();

        FloatTensor tensor = (FloatTensor) rows.get(0).getField(1);
        float aFloat = tensor.getFloat(20, 0);
        double number = (double) (Math.round(aFloat * 1E8) / 1E8);
        assertEquals(-7.3242E-4, number);
    }

}
