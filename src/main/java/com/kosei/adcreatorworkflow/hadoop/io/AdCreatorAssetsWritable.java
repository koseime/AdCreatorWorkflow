package com.kosei.adcreatorworkflow.hadoop.io;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by lanceriedel on 7/17/14.
 */
public class AdCreatorAssetsWritable implements Writable {
    public static final int STATUS_EMPTY = 0;
    public static final int STATUS_RAW = 1;
    public static final int STATUS_IMAGE_RETRIEVED = 2;

    private BytesWritable imageBlob;
    private ArrayWritable imageURIs;
    private Text id, productDesc, longProductDesc;
    private IntWritable status;
    private MapWritable meta;

    public AdCreatorAssetsWritable() {
        this.id = new Text();
        this.productDesc = new Text();
        this.longProductDesc = new Text();
        this.imageURIs = new ArrayWritable(UTF8.class);
        this.status = new IntWritable(STATUS_EMPTY);
        this.meta = new MapWritable();
        this.imageBlob = null;
    }


    public AdCreatorAssetsWritable(Text id, ArrayWritable imageURIs, IntWritable status, BytesWritable imageBlob, 
                                   Text productDesc, Text longProductDesc) {
        this.id = id;
        this.productDesc = productDesc;
        this.longProductDesc = longProductDesc;
        this.imageURIs = imageURIs;
        this.status = status;
        this.meta = new MapWritable();
        this.imageBlob = imageBlob;
    }

    public AdCreatorAssetsWritable(String id, String[] imageURIs, int status, byte[] imageBlob,
                                   String productDesc, String longProductDesc) {
        this.id = new Text(id);
        this.productDesc =  new Text(productDesc);
        this.longProductDesc =  new Text(longProductDesc);
        this.imageURIs = new ArrayWritable(imageURIs);
        this.status = new IntWritable(status);
        this.meta = new MapWritable();
        if (imageBlob == null) { imageBlob = new byte[0]; }
        this.imageBlob = new BytesWritable(imageBlob);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        productDesc.readFields(in);
        longProductDesc.readFields(in);
        imageURIs.readFields(in);
        status.readFields(in);
        meta = new MapWritable();
        meta.readFields(in);
        if (status.get() == 1) {
            imageBlob = new BytesWritable(new byte[0]);
        } else {
            imageBlob.readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        productDesc.write(out);
        longProductDesc.write(out);
        imageURIs.write(out);
        status.write(out);
        meta.write(out);
        if (status.get() != 1) {
            imageBlob.write(out);
        }
    }


    public BytesWritable getImageBlob() {
        return imageBlob;
    }

    public void setImageBlob(BytesWritable imageBlob) {
        this.imageBlob =  imageBlob;
    }

    public void setImageBlob(byte[] imageBlob) {
        this.imageBlob = new BytesWritable(imageBlob);
    }

    public Text getId() {
        return id;
    }

    public void setStatus(int status) {
        this.status = new IntWritable(status);
    }

    public void setStatus(IntWritable status) {
        this.status = status;
    }

    public ArrayWritable getImageURIs() {
        return imageURIs;
    }

    public Text getProductDesc() {
        return productDesc;
    }

    public Text getLongProductDesc() {
        return longProductDesc;
    }

    public IntWritable getStatus() {
        return status;
    }

    public Writable putMeta(Text key, BytesWritable value) { return meta.put(key, value); }

    public BytesWritable getMeta(Text key) { return (BytesWritable)meta.get(key); }

    public Set<Map.Entry<Writable, Writable>> entrySetMeta() { return meta.entrySet(); }
}
