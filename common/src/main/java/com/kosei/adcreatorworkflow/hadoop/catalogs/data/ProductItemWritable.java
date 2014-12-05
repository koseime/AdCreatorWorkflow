package com.kosei.adcreatorworkflow.hadoop.catalogs.data;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kosei.adcreatorworkflow.hadoop.GoogleProductItem;
import com.kosei.dropwizard.management.api.JsonCatalogVersion;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import io.dropwizard.jackson.Jackson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hess on 11/16/14.
 */
public class ProductItemWritable implements Writable {
  private static final ObjectMapper om = Jackson.newObjectMapper();

  public final LongWritable catalogVersionId;
  public final LongWritable catalogId;
  public final LongWritable advertiserId;
  public final LongWritable catalogTimestamp;
  public final Text productId;
  public final BytesWritable item;

  public ProductItemWritable(JsonCatalogVersion cv, GoogleProductItem item)
      throws JsonProcessingException {
    catalogVersionId = new LongWritable(cv.id);
    catalogId = new LongWritable(cv.catalogId);
    advertiserId = new LongWritable(cv.advertiserId);
    catalogTimestamp = new LongWritable(cv.timestamp);
    productId = new Text(item.getId());
    this.item = new BytesWritable(om.writeValueAsBytes(item));
  }

  public ProductItemWritable() {
    catalogVersionId = new LongWritable();
    catalogId = new LongWritable();
    advertiserId = new LongWritable();
    productId = new Text();
    catalogTimestamp = new LongWritable();
    item = new BytesWritable();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    catalogVersionId.write(out);
    catalogId.write(out);
    advertiserId.write(out);
    productId.write(out);
    catalogTimestamp.write(out);
    item.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    catalogVersionId.readFields(in);
    catalogId.readFields(in);
    advertiserId.readFields(in);
    productId.readFields(in);
    catalogTimestamp.readFields(in);
    item.readFields(in);
  }
}
