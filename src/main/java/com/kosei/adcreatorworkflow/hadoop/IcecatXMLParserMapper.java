package com.kosei.adcreatorworkflow.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author root
 */
public class IcecatXMLParserMapper extends
        Mapper<LongWritable, Text, NullWritable, AdCreatorAssetsWritable> {



    @Override
    public void map(LongWritable key, Text value1,Context context)

            throws IOException, InterruptedException {

        String xmlString = value1.toString();

        SAXBuilder builder = new SAXBuilder();
        Reader in = new StringReader(xmlString);
        String value="";
        try {

            Document doc = builder.build(in);
            Element root = doc.getRootElement();

            String productId =root.getChild("Product").getAttributeValue("Prod_id");
            String lowPicURI =root.getChild("Product").getAttributeValue("LowPic");
            String thumbPicURI =root.getChild("Product").getAttributeValue("ThumbPic");
            String productDesc = "";
            String longProductDesc = "";
            List<Element> childrenProduct = root.getChild("Product").getChildren("ProductDescription");

            for (Element e : childrenProduct) {
                if (e.getAttributeValue("langid") !=null && e.getAttributeValue("langid").equals("1")) {
                    productDesc = e.getAttributeValue("ShortDesc");
                    longProductDesc = e.getAttributeValue("LongDesc");
                }
            }

            AdCreatorAssetsWritable ad = new AdCreatorAssetsWritable(productId, lowPicURI,  thumbPicURI,  1, null,
            productDesc,  longProductDesc);


            context.write(NullWritable.get(), ad);
        } catch (JDOMException ex) {
            Logger.getLogger(IcecatXMLParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(IcecatXMLParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(IcecatXMLParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
