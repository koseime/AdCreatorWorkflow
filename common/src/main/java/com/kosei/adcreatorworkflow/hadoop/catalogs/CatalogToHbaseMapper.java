package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.kosei.adcreatorworkflow.hadoop.GoogleProductItem;
import com.kosei.adcreatorworkflow.hadoop.catalogs.data.ProductItemWritable;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import io.dropwizard.jackson.Jackson;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by hess on 11/17/14.
 */
public class CatalogToHbaseMapper
    extends Mapper<NullWritable, ProductItemWritable, BytesWritable, Put> {
  private final ObjectMapper om;
  private final ObjectReader reader;

  private final byte[] family;
  private final byte[] advertiserId;
  private final byte[] catalogId;
  private final byte[] catalogVersionId;
  private final byte[] catalogVersionTimestamp;
  private final byte[] productId;
  private final byte[] productDetails;

  private final byte[] inStock;
  private final byte[] title;
  private final byte[] description;


  public CatalogToHbaseMapper() {
    om = Jackson.newObjectMapper();
    reader = om.reader(GoogleProductItem.class);

    family = "c".getBytes();
    advertiserId = "advertiserId".getBytes();
    catalogId = "catalogId".getBytes();
    catalogVersionId = "catalogVersionId".getBytes();
    catalogVersionTimestamp = "catalogVersionTimestamp".getBytes();
    productId = "productId".getBytes();
    productDetails = "product".getBytes();
    inStock = "inStock".getBytes();
    title = "title".getBytes();
    description = "description".getBytes();
  }

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    String column = context.getConfiguration().get("conf.column");
  }

  @Override
  protected void map(NullWritable key, ProductItemWritable value, Context context)
      throws IOException, InterruptedException {

    ByteBuffer productDetailsBuffer = ByteBuffer.wrap(value.item.getBytes(), 0, value.item.getLength());
    try {
      byte[] rowkey = (value.catalogId+"-"+value.productId).getBytes();
      Put put = new Put(rowkey);
      put.add(family, advertiserId, toBytes(value.advertiserId.get()));
      put.add(family, catalogId, toBytes(value.catalogId.get()));
      put.add(family, catalogVersionId, toBytes(value.catalogVersionId.get()));
      put.add(family, catalogVersionTimestamp, toBytes(value.catalogTimestamp.get()));
      put.add(family, productId, value.productId.getBytes());
      put.add(family, ByteBuffer.wrap(productDetails), System.currentTimeMillis(), productDetailsBuffer);

      GoogleProductItem item = reader.readValue(value.item.getBytes());

      put.add(family, inStock, item.getAvailability().getBytes());
      put.add(family, title, item.getTitle().getBytes());

      context.write(new BytesWritable(rowkey), put);
      context.getCounter(CatalogUpdateCounter.HBASE_ROWS_ADDED).increment(1);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private byte[] toBytes(long value) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(value);
    return buffer.array();
  }

}
