package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kosei.adcreatorworkflow.hadoop.rest.BaseRestResourceMapper;
import com.kosei.adcreatorworkflow.hadoop.rest.ManagementClientResourceMapper;
import com.kosei.dropwizard.management.api.JsonCatalogVersion;
import com.kosei.dropwizard.management.api.Page;
import com.kosei.dropwizard.management.client.ManagementJsonClient;
import com.kosei.dropwizard.management.client.resources.CatalogVersionResource;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.jackson.Jackson;
import retrofit.client.UrlConnectionClient;

import java.io.IOException;

/**
 * Fetches catalog versions from the mapper and writes them out as JSON blobs indexed by
 * catalog version ID.
 *
 * Created by hess on 11/16/14.
 */
public class AllCatalogVersionsMapper extends ManagementClientResourceMapper<JsonCatalogVersion> {

  private CatalogVersionResource catalogVersionResource;
  AllCatalogVersionsMapper() {

  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    catalogVersionResource = managementJsonClient.getCatalogVersionResource();
  }

  @Override
  protected Page<JsonCatalogVersion> nextPage(long index) {
    return catalogVersionResource.viewAll(index, PAGE_SIZE);
  }
  @Override
  protected long getIdForObject(JsonCatalogVersion object) {
    return object.id;
  }
}
