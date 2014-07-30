package com.kosei.adcreatorworkflow.hadoop;

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
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * @author root
 */
public class PipesOutputParserReducer extends
        Reducer<NullWritable, BytesWritable, NullWritable, NullWritable> {

    @Override
    public void reduce(NullWritable key, Iterable<BytesWritable> values, Context context)

            throws IOException, InterruptedException {

        String tarOutput = context.getConfiguration().get("tar.output");
        Path file = new Path(tarOutput);
        FileSystem hdfs = FileSystem.get(file.toUri(), context.getConfiguration());
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

        context.write(NullWritable.get(), NullWritable.get());
    }

    public byte[] getValidBytes(BytesWritable bw) {
        return Arrays.copyOf(bw.getBytes(), bw.getLength());
    }

}
