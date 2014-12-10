package com.kosei.adcreatorworkflow.hadoop.rest;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hess on 12/9/14.
 */
public class RestResourceReducer extends
                                 Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {

  @Override
  protected void reduce(LongWritable key, Iterable<BytesWritable> values, Context context)
      throws IOException, InterruptedException {
    for(BytesWritable value : values) {
      context.write(key, value);
    }
  }

}
