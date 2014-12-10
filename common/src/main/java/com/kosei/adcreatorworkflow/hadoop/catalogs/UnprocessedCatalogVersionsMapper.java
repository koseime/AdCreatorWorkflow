package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kosei.adcreatorworkflow.hadoop.rest.BaseRestResourceMapper;
import com.kosei.adcreatorworkflow.hadoop.rest.ManagementClientResourceMapper;
import com.kosei.dropwizard.management.api.JsonCatalogVersion;
import com.kosei.dropwizard.management.api.JsonCatalogVersionPrepareJob;
import com.kosei.dropwizard.management.api.Page;
import com.kosei.dropwizard.management.api.Resource;
import com.kosei.dropwizard.management.client.ManagementJsonClient;
import com.kosei.dropwizard.management.client.resources.CatalogVersionPrepareJobResource;
import com.kosei.dropwizard.management.client.resources.CatalogVersionResource;

import org.apache.hadoop.mapreduce.Mapper;

import io.dropwizard.jackson.Jackson;
import retrofit.client.UrlConnectionClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by hess on 12/9/14.
 */
public class UnprocessedCatalogVersionsMapper extends ManagementClientResourceMapper<JsonCatalogVersion> {
  private String oozieJobId;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    oozieJobId = context.getConfiguration().get("oozie.job.id");

  }

  @Override
  protected Page nextPage(long index) {

    if (oozieJobId == null) { throw new RuntimeException("oozie.job.id not defined"); }
    long jobTimestamp = System.currentTimeMillis();

    CatalogVersionPrepareJobResource catalogVersionPrepareJobResource =
        managementJsonClient.getCatalogVersionPrepareJobResource();

    CatalogVersionResource
        catalogVersionResource = managementJsonClient.getCatalogVersionResource();

    JsonCatalogVersionPrepareJob newJob = catalogVersionPrepareJobResource.create(
        new JsonCatalogVersionPrepareJob(-1, oozieJobId, null, null, jobTimestamp, null));

    List<JsonCatalogVersion>
        catalogVersions =
        catalogVersionResource.viewAllByCatalogVersionPrepareJobId(newJob.id, 0, Integer.MAX_VALUE).content;


    return new Page(catalogVersions, new HashMap<String, Object>(0));
  }


  @Override
  protected long getIdForObject(JsonCatalogVersion object) {
    return object.id;
  }
}
