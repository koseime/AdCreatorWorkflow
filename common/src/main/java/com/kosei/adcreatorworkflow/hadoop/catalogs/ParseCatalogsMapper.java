package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.google.common.base.Charsets;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.kosei.adcreatorworkflow.hadoop.GoogleProductItem;
import com.kosei.adcreatorworkflow.hadoop.GoogleProductItemParser;
import com.kosei.adcreatorworkflow.hadoop.catalogs.data.ProductItemWritable;
import com.kosei.dropwizard.management.api.JsonCatalogVersion;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.jackson.Jackson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by hess on 11/17/14.
 */
public class ParseCatalogsMapper extends Mapper<LongWritable, BytesWritable, Text, ProductItemWritable> {
  private static final Logger log = LoggerFactory.getLogger(FetchCatalogUpdatesReducer.class);
  private ObjectMapper om;
  private AmazonS3Client s3Client;
  private ObjectReader reader;

  @Override
  protected void setup(Mapper.Context context) throws IOException, InterruptedException {
    super.setup(context);
    om = Jackson.newObjectMapper();
    AWSCredentials creds = new BasicAWSCredentials(
        context.getConfiguration().get("awsAccessKey"),
        context.getConfiguration().get("awsSecretKey"));
    s3Client = new AmazonS3Client(creds);
    reader = om.reader(JsonCatalogVersion.class);
  }

  @Override
  protected void map(LongWritable key, BytesWritable value, Context context)
      throws IOException, InterruptedException {

    JsonCatalogVersion cv = reader.readValue(value.getBytes());
    URI uri = null;
    try {
      uri = new URI(cv.catalogLocation);
    } catch (URISyntaxException e) {
      throw new IOException("Unable to parse s3n url: "+cv.catalogLocation,e);
    }

    if("s3n".equalsIgnoreCase(uri.getScheme())) {
      String bucketName = uri.getHost();
      String path = uri.getPath().substring(1); // strip leading '/' character
      S3Object response = s3Client.getObject(bucketName, path);
      long contentLength = response.getObjectMetadata().getContentLength();
      log.info("Reading {} ({} bytes)", path, contentLength);

      S3ObjectInputStream s3in = response.getObjectContent();
      BufferedReader br = new BufferedReader(new InputStreamReader(s3in,
                                                                   Charsets.UTF_8));
      String header = br.readLine();
      if(header != null) {
        GoogleProductItemParser parser = new GoogleProductItemParser(header);
        String line = br.readLine();
        while(line != null) {
          GoogleProductItem item = parser.parse(line);
          if(item != null) {
            context.write(new Text(item.getId() + "-" + cv.catalogId),
                          new ProductItemWritable(cv, item));
            context.getCounter(CatalogUpdateCounter.CATALOG_ITEM_MAPPED).increment(1);
          } else {
            log.info("Bad line in {} : {}", path, line);
            context.getCounter(CatalogUpdateCounter.CATALOG_ITEM_BAD_LINE).increment(1);
          }
          line = br.readLine();
        }
      }

      br.close();

    } else {
      throw new IllegalStateException("Expected s3n URL, got "+cv.catalogLocation);
    }

  }
}
