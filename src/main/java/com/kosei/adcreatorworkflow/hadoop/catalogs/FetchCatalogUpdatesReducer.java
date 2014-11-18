package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.kosei.dropwizard.management.api.JsonCatalogVersion;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.jackson.Jackson;

import java.io.IOException;

/**
 * Created by hess on 11/16/14.
 */
public class FetchCatalogUpdatesReducer extends
                                        Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {
  private static final Logger log = LoggerFactory.getLogger(FetchCatalogUpdatesReducer.class);
  private ObjectMapper om;
  private AmazonS3Client s3Client;
  private ObjectReader reader;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    om = Jackson.newObjectMapper();
    AWSCredentials creds = new BasicAWSCredentials(
        context.getConfiguration().get("awsAccessKey"),
        context.getConfiguration().get("awsSecretKey"));
    s3Client = new AmazonS3Client(creds);
    reader = om.reader(JsonCatalogVersion.class);
  }

  @Override
  protected void reduce(LongWritable key, Iterable<BytesWritable> values, Context context)
      throws IOException, InterruptedException {
    long catalogVersionId = key.get();
    for(BytesWritable value : values) {
      context.write(key, value);
    }
  }
}
