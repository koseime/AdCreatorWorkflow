package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 *
 */
public class UploadProductsToCategoriesMapper extends
        Mapper<NullWritable, AdCreatorAssetsWritable, Text, Text> {
    @Override
    public void map(NullWritable key, AdCreatorAssetsWritable value, Context context)
            throws InterruptedException, IOException {
        String catalogId = new String(value.getMeta(new Text("catalog_id")).copyBytes());
        String productId = value.getId().toString();
        String category = new String(value.getMeta(new Text("category")).copyBytes());
        context.write(new Text(catalogId), new Text(productId + "=" + category));
    }
}
