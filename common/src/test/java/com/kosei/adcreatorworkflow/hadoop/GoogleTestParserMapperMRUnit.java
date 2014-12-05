package com.kosei.adcreatorworkflow.hadoop;


import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
//TO RUN:
//-Djava.security.krb5.realm=OX.AC.UK -Djava.security.krb5.kdc=kdc0.ox.ac.uk:kdc1.ox.ac.uk

public class GoogleTestParserMapperMRUnit {


    @Before
    public void setUp() {


    }

    /* TODO: fix test
    @Test
    public void testMapper() throws Exception {
        MapDriver<LongWritable, Text, NullWritable, AdCreatorAssetsWritable> mapDriver;

        GoogleTextParserMapper mapper = new GoogleTextParserMapper();
        mapDriver = MapDriver.newMapDriver(mapper);


        System.setProperty("HADOOP_USER_NAME", "hduser");

        System.setSecurityManager(null);
        InputStream in = getClass().getResourceAsStream("/google-input.txt");
        BufferedReader b = new BufferedReader(new InputStreamReader(in));
        String line = b.readLine();
        System.out.println(line);


        mapDriver.withInput(new LongWritable(), new Text(
                line));

        //    mapDriver.withOutput(new Text("6"), mockedDocFeaturesWritable);
        List<Pair<NullWritable, AdCreatorAssetsWritable>> res = mapDriver.run();
        Assert.assertTrue(res.size()==1);


        System.out.println("done");
    }
    */


}
