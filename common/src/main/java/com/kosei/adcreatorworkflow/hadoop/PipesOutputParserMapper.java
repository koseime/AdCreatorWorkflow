package com.kosei.adcreatorworkflow.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author root
 */
public class PipesOutputParserMapper extends
        Mapper<Text, Text, NullWritable, BytesWritable> {

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(NullWritable.get(), new BytesWritable(getValidBytes(value)));
    }

    public byte[] getValidBytes(Text text) {
        return Arrays.copyOf(text.getBytes(), text.getLength());
    }

}
