package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.dropwizard.management.api.JsonCatalogVersion;
import com.kosei.dropwizard.management.api.JsonCatalogVersionPrepareJob;
import com.kosei.dropwizard.management.client.ManagementJsonClient;
import com.kosei.dropwizard.management.client.resources.CatalogVersionPrepareJobResource;
import com.kosei.dropwizard.management.client.resources.CatalogVersionResource;
import io.dropwizard.jackson.Jackson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.client.UrlConnectionClient;

import java.io.*;
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
        String basePath = args[2];

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

        basePath += "/" + Long.toString(jobTimestamp);
        Path baseDir = new Path(basePath);
        FileSystem hdfs = FileSystem.get(baseDir.toUri(), new Configuration());
        hdfs.mkdirs(baseDir);

        Path catalogLocationsFile = new Path(basePath + "/catalog_locations.txt");
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(catalogLocationsFile)));
        br.write(catalogLocations.toString());
        br.close();

        Path catalogNamesFile = new Path(basePath + "/catalog_names.txt");
        br = new BufferedWriter(new OutputStreamWriter(hdfs.create(catalogNamesFile)));
        br.write(catalogNames.toString());
        br.close();

        String propertiesFile = System.getProperty("oozie.action.output.properties");
        if (propertiesFile != null) {
            File propFile = new File(propertiesFile);

            Properties properties = new Properties();
            properties.setProperty("TIMESTAMP", Long.toString(jobTimestamp));

            OutputStream os = new FileOutputStream(propFile);
            properties.store(os, "");
            os.close();
        } else {
            throw new RuntimeException("oozie.action.output.properties not defined");
        }
    }

}
