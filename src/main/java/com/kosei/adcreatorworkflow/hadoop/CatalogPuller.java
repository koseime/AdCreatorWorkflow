package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.dropwizard.management.api.JsonCatalogVersion;
import com.kosei.dropwizard.management.api.JsonCatalogVersionPrepareJob;
import com.kosei.dropwizard.management.client.ManagementJsonClient;
import com.kosei.dropwizard.management.client.resources.CatalogVersionPrepareJobResource;
import com.kosei.dropwizard.management.client.resources.CatalogVersionResource;
import io.dropwizard.jackson.Jackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.client.UrlConnectionClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;

/**
 *
 *
 */
public class CatalogPuller {
    private static final Logger log = LoggerFactory.getLogger(CatalogPuller.class);

    public static void main(String args[]) throws IOException {
        String apiToken = args[0];
        String baseURL = args[1];

        log.info("Initiating catalog pull base URL: {}, API: {}", apiToken, baseURL);

        String oozieJobId = System.getProperty("oozie.job.id");
        if (oozieJobId == null) { throw new RuntimeException("oozie.job.id not defined"); }
        long jobTimestamp = System.currentTimeMillis();

        ManagementJsonClient managementJsonClient = new ManagementJsonClient(
                apiToken, baseURL, Jackson.newObjectMapper(), new UrlConnectionClient());

        CatalogVersionPrepareJobResource catalogVersionPrepareJobResource =
                managementJsonClient.getCatalogVersionPrepareJobResource();

        CatalogVersionResource catalogVersionResource = managementJsonClient.getCatalogVersionResource();

        JsonCatalogVersionPrepareJob newJob = catalogVersionPrepareJobResource.create(
                new JsonCatalogVersionPrepareJob(-1, oozieJobId, null, null, jobTimestamp, null));

        List<JsonCatalogVersion> catalogVersions =
                catalogVersionResource.viewAllByCatalogVersionPrepareJobId(newJob.id, 0, Integer.MAX_VALUE).content;

        StringBuilder catalogLocations = new StringBuilder();
        StringBuilder catalogNames = new StringBuilder();

        for (JsonCatalogVersion catalogVersion : catalogVersions) {
            log.info("Retrieved CatalogVersion {}", catalogVersion);
            if (catalogLocations.length() != 0) {
                catalogLocations.append(",");
                catalogNames.append(",");
            }
            String catalogName = catalogVersion.advertiserId + "-" + catalogVersion.timestamp + "-" +
                    catalogVersion.catalogId + "-" + catalogVersion.id + ".raw_catalog";
            catalogLocations.append(catalogVersion.catalogLocation);
            catalogNames.append(catalogName);
        }

        String propertiesFile = System.getProperty("oozie.action.output.properties");
        if (propertiesFile != null) {
            File propFile = new File(propertiesFile);

            Properties properties = new Properties();
            properties.setProperty("CATALOG_LOCATIONS", catalogLocations.toString());
            properties.setProperty("CATALOG_NAMES", catalogNames.toString());
            properties.setProperty("TIMESTAMP", Long.toString(jobTimestamp));

            OutputStream os = new FileOutputStream(propFile);
            properties.store(os, "");
            os.close();
        } else {
            throw new RuntimeException("oozie.action.output.properties not defined");
        }
    }

}
