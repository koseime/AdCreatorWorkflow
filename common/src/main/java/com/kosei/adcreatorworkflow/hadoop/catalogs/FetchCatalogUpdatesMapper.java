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
  private CatalogVersionResource catalogVersionResource;
  FetchCatalogUpdatesMapper() {
    System.err.println("FetchCatalogUpdatesMapper Constructor");
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    System.err.println("FetchCatalogUpdatesMapper setup() start");
    try {
      log.info("Configuring mapper context");
      this.om = Jackson.newObjectMapper();
      super.setup(context);
      String apiToken = context.getConfiguration().get("managementClientApiToken");
      String baseURL = context.getConfiguration().get("managementClientApiEndPoint");

      ManagementJsonClient managementJsonClient = new ManagementJsonClient(
          apiToken, baseURL, Jackson.newObjectMapper(), new UrlConnectionClient());

      catalogVersionResource = managementJsonClient.getCatalogVersionResource();
      // run a test query to attempt to get a single catalog item
      catalogVersionResource.viewAll(0, 1);
      log.info("FetchCatalogUpdater.setup() completed successfully");
    } catch(Exception e) {
      System.err.println("FetchCatalogUpdatesMapper setup() exception");
      log.error("Unable to query the catalog resource", e);
    }
    System.err.println("FetchCatalogUpdatesMapper setup() exit");
  }

  @Override
  protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
    System.err.println("FetchCatalogUpdatesMapper map() enter");
    int localCounter = 0;


    Page<JsonCatalogVersion> catalogVersionsPage;
    long index = 0;
    do {

      catalogVersionsPage = catalogVersionResource.viewAll(index, PAGE_SIZE);
      System.err.println("FetchCatalogUpdatesMapper map() versionsPage: "+catalogVersionsPage);
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

    System.err.println("FetchCatalogUpdatesMapper map() exit");

  }
}
