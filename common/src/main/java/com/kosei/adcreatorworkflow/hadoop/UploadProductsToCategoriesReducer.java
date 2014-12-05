package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.dropwizard.management.client.ManagementJsonClient;
import com.kosei.dropwizard.management.client.resources.ProductCategoriesMetaResource;
import io.dropwizard.jackson.Jackson;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import retrofit.client.UrlConnectionClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 *
 */
public class UploadProductsToCategoriesReducer extends
        Reducer<Text, Text, NullWritable, NullWritable> {

    private final static Logger LOG = Logger.getLogger(UploadProductsToCategoriesReducer.class.getName());

    public static final int BATCH_SIZE = 500;

    private ProductCategoriesMetaResource productCategoriesMetaResource = null;

    @Override
    public void setup(Context context) {
        String apiToken = context.getConfiguration().get("api.token");
        String baseURL = context.getConfiguration().get("base.url");

        ManagementJsonClient managementJsonClient = new ManagementJsonClient(
                apiToken, baseURL, Jackson.newObjectMapper(), new UrlConnectionClient());

        productCategoriesMetaResource = managementJsonClient.getProductCategoriesMetaResource();

    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long catalogId = Long.parseLong(key.toString());

        Map<String, List<String>> productsToCategoriesBatch = new HashMap<String, List<String>>();
        for (Text value : values) {
            String[] keyValue = value.toString().split("=");
            String[] categories = keyValue[1].split(",");
            productsToCategoriesBatch.put(keyValue[0].trim(), Arrays.asList(categories));

            if (productsToCategoriesBatch.size() >= BATCH_SIZE) {
                List<String> failedItems = productCategoriesMetaResource.createProductToCategoriesMappings(
                        catalogId, productsToCategoriesBatch);
                LOG.log(Level.SEVERE, "Failed to upload categories for :" + failedItems);
                productsToCategoriesBatch.clear();
            }
        }

        if (!productsToCategoriesBatch.isEmpty()) {
            List<String> failedItems = productCategoriesMetaResource.createProductToCategoriesMappings(
                    catalogId, productsToCategoriesBatch);
            LOG.log(Level.SEVERE, "Failed to upload categories for :" + failedItems);
        }
    }
}
