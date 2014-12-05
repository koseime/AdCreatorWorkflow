package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kosei.dropwizard.management.api.JsonCatalogVersion;
import com.kosei.dropwizard.management.api.JsonCatalogVersionPrepareJob;
import com.kosei.dropwizard.management.api.Page;
import com.kosei.dropwizard.management.client.ManagementJsonClient;
import com.kosei.dropwizard.management.client.resources.CatalogVersionPrepareJobResource;
import com.kosei.dropwizard.management.client.resources.CatalogVersionResource;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.jackson.Jackson;
import retrofit.client.UrlConnectionClient;

import java.io.IOException;
import java.util.List;

/**
 * Fetches catalog versions from the mapper and writes them out as JSON blobs indexed by
 * catalog version ID.
 *
 * Created by hess on 11/16/14.
 */
public class FetchCatalogUpdatesMapper extends
                                       Mapper<NullWritable, NullWritable, LongWritable, BytesWritable> {
  private static final Logger log = LoggerFactory.getLogger(FetchCatalogUpdatesMapper.class);
  private static final int PAGE_SIZE = 100;
  private static final int MAX_RECORDS = Integer.MAX_VALUE;
  private ObjectMapper om;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.om = Jackson.newObjectMapper();
    super.setup(context);
  }

  @Override
  protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
    int localCounter = 0;

    String apiToken = context.getConfiguration().get("managementClientApiToken");
    String baseURL = context.getConfiguration().get("managementClientApiEndPoint");

    ManagementJsonClient managementJsonClient = new ManagementJsonClient(
        apiToken, baseURL, Jackson.newObjectMapper(), new UrlConnectionClient());

    CatalogVersionResource
        catalogVersionResource = managementJsonClient.getCatalogVersionResource();

    Page<JsonCatalogVersion> catalogVersionsPage;
    long index = 0;
    do {

      catalogVersionsPage = catalogVersionResource.viewAll(index, PAGE_SIZE);
      for(JsonCatalogVersion cv : catalogVersionsPage.content) {
        try {
          localCounter++;
          context.getCounter(CatalogUpdateCounter.FETCH_CATALOG_VERSION_COUNT).increment(1L);
          context.write(new LongWritable(cv.id), new BytesWritable(om.writeValueAsBytes(cv)));
        } catch (IOException e) {
          context.getCounter(CatalogUpdateCounter.FETCH_CATALOG_BAD_RECORD).increment(1L);
        } catch (InterruptedException e) {
          context.getCounter(CatalogUpdateCounter.FETCH_CATALOG_BAD_RECORD).increment(1L);
        }
      }

      index += PAGE_SIZE;
    } while(catalogVersionsPage != null && catalogVersionsPage.getNextLink() != null && localCounter < MAX_RECORDS) ;


  }
}
