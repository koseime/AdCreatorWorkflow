package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;

/**
 * @author root
 */
public class PipesOutputParserReducer extends
        Reducer<NullWritable, BytesWritable, NullWritable, Text> {

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

        for (BytesWritable value : values) {
            AdComponents adComponents = AdComponents.parseFrom(getValidBytes(value));

            StringBuilder productIdImageNameListStringBuilder = new StringBuilder(adComponents.getId());
            if (!isDeleted(adComponents)) {
                for (int i = 0; i < adComponents.getGeneratedAdsCount(); i++) {
                    AdComponents.Ad ad = adComponents.getGeneratedAds(i);
                    String file_name = adComponents.getId() + "_" + ad.getLayoutName() + ".jpg";
                    TarArchiveEntry tarEntry = new TarArchiveEntry(file_name);
                    tarEntry.setSize(ad.getAdJpg().size());
                    tarOut.putArchiveEntry(tarEntry);

                    ByteArrayInputStream imageStream = new ByteArrayInputStream(ad.getAdJpg().toByteArray());
                    IOUtils.copy(imageStream, tarOut);
                    imageStream.close();
                    tarOut.closeArchiveEntry();

                    if (i == 0) {
                        productIdImageNameListStringBuilder.append('=');
                    } else {
                        productIdImageNameListStringBuilder.append(',');
                    }
                    productIdImageNameListStringBuilder.append(file_name);
                }
            } else {
                productIdImageNameListStringBuilder.append("=DELETED");
            }

            context.write(NullWritable.get(), new Text(productIdImageNameListStringBuilder.toString()));
        }
        tarOut.finish();
        tarOut.close();

        // TODO: remove these?
        gzOut.close();
        bufferedOut.close();
        out.close();
    }

    public byte[] getValidBytes(BytesWritable bw) {
        return Arrays.copyOf(bw.getBytes(), bw.getLength());
    }

    private boolean isDeleted(AdComponents adComponents) {
        for (int i = 0; i < adComponents.getMetaCount(); i++) {
            AdComponents.Meta metaEntry = adComponents.getMeta(i);
            if (metaEntry.getKey().equals("deleted")) {
                return true;
            }
        }
        return false;
    }

}
