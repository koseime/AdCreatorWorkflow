package com.kosei.adcreatorworkflow.hadoop;

import com.google.common.base.Charsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.kosei.adcreatorworkflow.hadoop.catalogs.data.ProductItemWritable;
import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import io.dropwizard.jackson.Jackson;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hess on 12/9/14.
 */
public class ConvertToAdCreatorAssetMapper extends Mapper<
    NullWritable, ProductItemWritable,
    NullWritable, AdCreatorAssetsWritable> {
  public static enum ParserEnum {SUCCESS}

  private static final ObjectMapper om = Jackson.newObjectMapper();
  private final ObjectReader reader = om.reader(GoogleProductItem.class);

  long timestamp;
  byte[] timestampBytes;

  @Override
  public void setup(Context context) {

    String timestampString = context.getConfiguration().get("catalog.timestamp");
    timestamp = Long.parseLong(timestampString);
    timestampBytes = longToBytes(timestamp);
  }

  private static byte[] longToBytes(long timestamp) {
    return Long.toString(timestamp).getBytes(Charsets.US_ASCII);
  }
  private static byte[] longToBytes(LongWritable timestamp) {
    return longToBytes(timestamp.get());
  }

  @Override
  protected void map(NullWritable key, ProductItemWritable value, Context context)
      throws IOException, InterruptedException {

    GoogleProductItem gpi = reader.readValue(value.item.getBytes());

    String productId = gpi.getId();
    String imageURI = gpi.getImageLink();
    String additionalImageURIs = gpi.getAdditionalImageLink();
    String productDesc = gpi.getTitle();
    String longProductDesc = gpi.getDescription();
    String category = cleanCategory(gpi.getGoogleProductCategory());
    String availability = gpi.getAvailability();

    if (productId.isEmpty()) { return; }
    AdCreatorAssetsWritable ad = new AdCreatorAssetsWritable(
        productId,
        GoogleProductItemParser.getAllImageURIs(imageURI, additionalImageURIs),
        AdCreatorAssetsWritable.STATUS_RAW, null, productDesc, longProductDesc);

    ad.putMeta(new Text("timestamp"), new BytesWritable(timestampBytes));
    ad.putMeta(new Text("advertiser_id"), new BytesWritable(longToBytes(value.advertiserId)));
    ad.putMeta(new Text("catalog_id"), new BytesWritable(longToBytes(value.catalogId)));
    ad.putMeta(new Text("catalog_version_id"), new BytesWritable(longToBytes(value.catalogVersionId)));
    ad.putMeta(new Text("catalog_timestamp"), new BytesWritable(longToBytes(value.catalogTimestamp)));
    ad.putMeta(new Text("category"), new BytesWritable(gpi.getGoogleProductCategory().getBytes(
        Charsets.UTF_8)));

    if (availability.equals("out of stock")) {
      ad.putMeta(new Text("category"), new BytesWritable("DELETED".getBytes()));
      ad.putMeta(new Text("deleted"), new BytesWritable(availability.getBytes()));
    } else {
      ad.putMeta(new Text("category"), new BytesWritable(category.getBytes()));
    }

    context.write(NullWritable.get(), ad);
    context.getCounter(ParserEnum.SUCCESS).increment(1);

  }


  private String getCatalogHeader(String filename) throws FileNotFoundException, IOException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String catalogHeader = br.readLine();
    br.close();
    return catalogHeader;
  }

  private String cleanCategory(String category) {
    category = category.replaceAll("(\\s*)>(\\s*)", ">");
    category = category.trim();
    return category;
  }
}
