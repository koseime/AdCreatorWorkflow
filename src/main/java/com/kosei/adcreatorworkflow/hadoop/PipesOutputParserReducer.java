package com.kosei.adcreatorworkflow.hadoop;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.apache.commons.io.IOUtils;

/**
 * @author root
 */
public class PipesOutputParserReducer extends
        Reducer<NullWritable, BytesWritable, NullWritable, NullWritable> {

    @Override
    public void reduce(NullWritable key, Iterable<BytesWritable> values, Context context)

            throws IOException, InterruptedException {


        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(configuration);
        Path file = new Path("hdfs://localhost:9000/tmp/img.tar.gz");
        if (hdfs.exists(file)) { hdfs.delete(file, true); }
        FSDataOutputStream out = hdfs.create(file);
        BufferedOutputStream bufferedOut = new BufferedOutputStream(out);
        GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(bufferedOut);
        TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzOut);

        for(BytesWritable value : values) {
            AdComponents ad = AdComponents.parseFrom(getValidBytes(value));

            String file_name = ad.getId() + ".jpg";
            TarArchiveEntry tarEntry = new TarArchiveEntry(file_name);
            tarEntry.setSize(ad.getGeneratedJpgAd().size());
            tarOut.putArchiveEntry(tarEntry);

            ByteArrayInputStream imageStream= new ByteArrayInputStream(ad.getGeneratedJpgAd().toByteArray());
            IOUtils.copy(imageStream , tarOut);
            imageStream.close();
            tarOut.closeArchiveEntry();
        }
        tarOut.finish();
        tarOut.close();
        gzOut.close();
        bufferedOut.close();
        out.close();

        /*
        FileOutputStream output = new FileOutputStream(new File("/tmp/" + ad.getId() + "img" + Integer.toString(counter) + ".jpg"));
        output.write(ad.getGeneratedJpgAd().toByteArray());
        output.close();
        */

    }

    public byte[] getValidBytes(BytesWritable text) {
        return Arrays.copyOf(text.getBytes(), text.getLength());
    }

    public byte[] objectToByteBuffer(Object o) throws Exception {
        Message message = (Message) o;

        byte[] messageBytes = message.toByteArray();
        return messageBytes;
    }

}
