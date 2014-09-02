package com.kosei.adcreatorworkflow.hadoop;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by chantat on 9/2/14.
 */
public class GoogleProductItemParser {
    private static final List<String> columnNameList = Arrays.asList(
            "id",
            "image_link",
            "additional_image_link",
            "title",
            "description",
            "google_product_category",
            "availability"
    );
    private final Map<String, Integer> columnNameToId;

    private int idToIndex[] = null;
    private String header = null;

    public GoogleProductItemParser(String header) {
        ImmutableMap.Builder<String, Integer> columnNameToIdBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < columnNameList.size(); i++) {
            columnNameToIdBuilder.put(columnNameList.get(i), i);
        }
        columnNameToId = columnNameToIdBuilder.build();

        this.header = header;

        String s[] = header.split("\t");
        idToIndex = new int[GoogleProductItem.NUM_PRODUCT_ITEM_ENTRIES];
        Arrays.fill(idToIndex, s.length);
        for (int i = 0; i < s.length; i++) {
            if (columnNameToId.containsKey(s[i])) {
                int id = columnNameToId.get(s[i]);
                idToIndex[id] = i;
            }
        }
    }

    public GoogleProductItem parse(String in) {
        if (header.isEmpty()) {
            return GoogleProductItem.fromParse(in);
        }
        List<String> s = Arrays.asList(in.split("\t"));
        s.add("");
        GoogleProductItem gpi = new GoogleProductItem();
        gpi.setId(s.get(idToIndex[0]));
        gpi.setImageLink(s.get(idToIndex[1]));
        gpi.setAdditionalImageLink(s.get(idToIndex[2]));
        gpi.setTitle(s.get(idToIndex[3]));
        gpi.setDescription(s.get(idToIndex[4]));
        gpi.setGoogleProductCategory(s.get(idToIndex[5]));
        gpi.setAvailability(s.get(idToIndex[6]));
        return gpi;
    }
}
