package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.kosei.adcreatorworkflow.hadoop.catalogs.data.ProductItemWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by hess on 11/17/14.
 */
public class ParseCatalogsReducer extends
                                  Reducer<Text, ProductItemWritable, NullWritable, ProductItemWritable> {
  private static final Logger log = LoggerFactory.getLogger(ParseCatalogsReducer.class);


  @Override
  protected void reduce(Text key, Iterable<ProductItemWritable> values,
                        Context context) throws IOException, InterruptedException {
    log.info("Processing item "+key);
    ProductItemWritable newestValue = null;
    long newestTimestamp = 0;
    int counter = 0;
    for(ProductItemWritable value : values) {
      if(value.catalogTimestamp.get() > newestTimestamp) {
        newestValue = value;
        newestTimestamp = value.catalogTimestamp.get();
      }
      counter++;
      context.getCounter(CatalogUpdateCounter.CATALOG_ITEM_REDUCED).increment(1);
    }
    log.info("Processed item "+key+" "+counter+" items");
    if(newestValue != null) {
      context.getCounter(CatalogUpdateCounter.CATALOG_ITEM_UNIQUE).increment(1);
      context.write(NullWritable.get(), newestValue);
    }
  }
}
