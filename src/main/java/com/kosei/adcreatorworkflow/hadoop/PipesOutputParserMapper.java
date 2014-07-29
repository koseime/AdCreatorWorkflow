package com.kosei.adcreatorworkflow.hadoop;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;


import java.io.FileOutputStream;
import java.io.File;
import org.apache.commons.io.IOUtils;

/**
 * @author root
 */
public class PipesOutputParserMapper extends
        Mapper<Text, Text, NullWritable, BytesWritable> {

    int counter = 0;

    @Override
    public void map(Text key, Text value, Context context)

            throws IOException, InterruptedException {

        counter++;

        AdComponents ad = AdComponents.parseFrom(getValidBytes(value));

        /*Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(configuration);
        Path file = new Path("hdfs://localhost:9000/tmp/" + ad.getId() + "img" + Integer.toString(counter) + ".jpg");
        if (hdfs.exists(file)) { hdfs.delete(file, true); }
        FSDataOutputStream out = hdfs.create(file);
        out.write(ad.getGeneratedJpgAd().toByteArray(), 0, ad.getGeneratedJpgAd().size());
        out.close();*/

        /*
        FileOutputStream output = new FileOutputStream(new File("/tmp/" + ad.getId() + "img" + Integer.toString(counter) + ".jpg"));
        output.write(ad.getGeneratedJpgAd().toByteArray());
        output.close();
        */
        context.write(NullWritable.get(), new BytesWritable(getValidBytes(value)));

    }

    public byte[] getValidBytes(Text text) {
        return Arrays.copyOf(text.getBytes(), text.getLength());
    }

    public byte[] objectToByteBuffer(Object o) throws Exception {
        Message message = (Message) o;

        byte[] messageBytes = message.toByteArray();
        return messageBytes;
    }

}
