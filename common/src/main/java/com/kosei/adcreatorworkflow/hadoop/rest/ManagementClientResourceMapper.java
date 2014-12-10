package com.kosei.adcreatorworkflow.hadoop.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kosei.adcreatorworkflow.hadoop.catalogs.AllCatalogVersionsMapper;
import com.kosei.dropwizard.management.api.Resource;
import com.kosei.dropwizard.management.client.ManagementJsonClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.jackson.Jackson;
import retrofit.client.UrlConnectionClient;

import java.io.IOException;

/**
 * Created by hess on 12/9/14.
 */
public abstract class ManagementClientResourceMapper<T extends Resource> extends BaseRestResourceMapper<T> {
  private static final Logger log = LoggerFactory.getLogger(ManagementClientResourceMapper.class);
  private ObjectMapper om;

  protected ManagementJsonClient managementJsonClient;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    System.err.println("FetchCatalogUpdatesMapper setup() start");
    try {
      log.info("Configuring mapper context");
      this.om = Jackson.newObjectMapper();
      super.setup(context);
      String apiToken = context.getConfiguration().get("managementClientApiToken");
      String baseURL = context.getConfiguration().get("managementClientApiEndPoint");

      managementJsonClient = new ManagementJsonClient(
          apiToken, baseURL, Jackson.newObjectMapper(), new UrlConnectionClient());

      log.info("FetchCatalogUpdater.setup() completed successfully");
    } catch(Exception e) {
      System.err.println("FetchCatalogUpdatesMapper setup() exception");
      log.error("Unable to query the catalog resource", e);
    }
    System.err.println("FetchCatalogUpdatesMapper setup() exit");
  }

  @Override
  protected ObjectMapper getObjectMapper() {
    return this.om;
  }


}
