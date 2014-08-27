package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by chantat on 8/27/14.
 */
public class CatalogCompactorMapper extends
        Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    @Override
    public void map(NullWritable key, BytesWritable value, Context context)
            throws IOException, InterruptedException {

        AdComponents adComponents = AdComponents.parseFrom(value.copyBytes());
        context.write(new Text(adComponents.getId()), value);
    }
}
