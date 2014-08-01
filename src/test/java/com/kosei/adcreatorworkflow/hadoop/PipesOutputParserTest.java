package com.kosei.adcreatorworkflow.hadoop;

import com.google.protobuf.ByteString;
import com.kosei.proto.AdComponentsMessages.AdComponents;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PipesOutputParserTest {

    MapDriver<Text, Text, NullWritable, BytesWritable> mapDriver;
    ReduceDriver<NullWritable, BytesWritable, NullWritable, NullWritable> reduceDriver;
    MapReduceDriver<Text, Text, NullWritable, BytesWritable, NullWritable, NullWritable> mapReduceDriver;

    Configuration conf;
    AdComponents adComponents1, adComponents2;

    List<BytesWritable> values;

    @Before
    public void setUp() throws IOException {
        PipesOutputParserMapper mapper = new PipesOutputParserMapper();
        PipesOutputParserReducer reducer = new PipesOutputParserReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        conf = new Configuration();
        reduceDriver.setConfiguration(conf);
        conf.set("tar.output", "hdfs://localhost:9000/tmp/test.tar.gz");

        System.setProperty("HADOOP_USER_NAME", "chantat");

        System.setSecurityManager(null);

        InputStream in1 = getClass().getResourceAsStream("/vacuum1.jpg");
        byte[] image1ByteArray = IOUtils.toByteArray(in1);
        AdComponents.Ad ad1 = AdComponents.Ad.newBuilder()
                .setLayoutName("ad1")
                .setAdJpg(ByteString.copyFrom(image1ByteArray))
                .build();
        adComponents1 = AdComponents.newBuilder()
                .setId("test product 1")
                .setDescription("test description 1")
                .addGeneratedAds(ad1)
                .setStatus(AdComponents.Status.IMAGE_RETRIEVED)
                .build();

        InputStream in2 = getClass().getResourceAsStream("/vacuum2.jpg");
        byte[] image2ByteArray = IOUtils.toByteArray(in2);
        AdComponents.Ad ad2 = AdComponents.Ad.newBuilder()
                .setLayoutName("ad2")
                .setAdJpg(ByteString.copyFrom(image2ByteArray))
                .build();
        adComponents2 = AdComponents.newBuilder()
                .setId("test product 2")
                .setDescription("test description 2")
                .addGeneratedAds(ad2)
                .setStatus(AdComponents.Status.IMAGE_RETRIEVED)
                .build();

        values = new ArrayList<BytesWritable>();
        values.add(new BytesWritable(adComponents1.toByteArray()));
        values.add(new BytesWritable(adComponents2.toByteArray()));
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new Text(""), new Text(adComponents1.toByteArray()));
        mapDriver.withOutput(NullWritable.get(), new BytesWritable(adComponents1.toByteArray()));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException, InterruptedException {
        reduceDriver.withInput(NullWritable.get(), values);
        reduceDriver.withOutput(NullWritable.get(), NullWritable.get());
        reduceDriver.runTest();

        // Verify tar.gz file
        Path file = new Path("hdfs://localhost:9000/tmp/test.tar.gz");
        FileSystem hdfs = FileSystem.get(file.toUri(), conf);
        assertTrue(hdfs.exists(file));

        FSDataInputStream in = hdfs.open(file);
        BufferedInputStream bufferedIn = new BufferedInputStream(in);
        GzipCompressorInputStream gzIn = new GzipCompressorInputStream(bufferedIn);
        TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);

        assertNotNull(tarIn.getNextTarEntry());
        ByteArrayOutputStream imageStream1 = new ByteArrayOutputStream();
        IOUtils.copy(tarIn, imageStream1);
        assertTrue(Arrays.equals(adComponents1.getGeneratedAds(0).getAdJpg().toByteArray(), imageStream1.toByteArray()));

        assertNotNull(tarIn.getNextTarEntry());
        ByteArrayOutputStream imageStream2 = new ByteArrayOutputStream();
        IOUtils.copy(tarIn, imageStream2);
        assertTrue(Arrays.equals(adComponents2.getGeneratedAds(0).getAdJpg().toByteArray(), imageStream2.toByteArray()));

        assertNull(tarIn.getNextEntry());

        tarIn.close();
        gzIn.close();
        bufferedIn.close();
        in.close();
    }
}