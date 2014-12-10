package com.kosei.adcreatorworkflow.hadoop;

import java.util.*;

/**
 *
 *
 */
public class KoseiProductItem {
    private String id;
    private String imageLink;
    private String additionalImageLink;
    private String title;
    private String description;
    private String googleProductCategory;
    private String availability;
    private String timestamp;
    private String catalogId;
    private String advertiserId;

    public static KoseiProductItem parse(GoogleProductItem googleProductItem, String timestamp, String catalogId,
                                         String advertiserId) {
        KoseiProductItem productItem = new KoseiProductItem();
        productItem.id = googleProductItem.getId();
        productItem.imageLink = googleProductItem.getImageLink();
        productItem.additionalImageLink = googleProductItem.getAdditionalImageLink();
        productItem.title = googleProductItem.getTitle();
        productItem.description = googleProductItem.getDescription();
        productItem.googleProductCategory = googleProductItem.getGoogleProductCategory();
        productItem.availability = googleProductItem.getAvailability();
        productItem.timestamp = timestamp;
        productItem.catalogId = catalogId;
        productItem.advertiserId = advertiserId;

        return productItem;
    }

    public static KoseiProductItem parse(String in) {
        KoseiProductItem productItem = new KoseiProductItem();
        // split will not include empty strings at the end, we add non-empty string at the end as a hack
        String[] s = (in + "\t$").split("\t");
        
        productItem.id = s[0];
        productItem.imageLink = s[1];
        productItem.additionalImageLink = s[2];
        productItem.title = s[3];
        productItem.description = s[4];
        productItem.googleProductCategory = s[5];
        productItem.availability = s[6];
        productItem.timestamp = s[7];
        productItem.catalogId = s[8];
        productItem.advertiserId = s[9];

        return productItem;
    }

    public String[] getAllImageURIs() {
        ArrayList<String> s = new ArrayList<String>();
        if (!imageLink.isEmpty()) {
            s.add(imageLink);
        }
        if (!additionalImageLink.isEmpty()) {
            s.addAll(Arrays.asList(additionalImageLink.split(",")));
        }
        return s.toArray(new String[s.size()]);
    }

    public String getCleanCategory() {
        return googleProductCategory.replaceAll("(\\s*)>(\\s*)", ">").trim();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getImageLink() {
        return imageLink;
    }

    public void setImageLink(String imageLink) {
        this.imageLink = imageLink;
    }

    public String getAdditionalImageLink() {
        return additionalImageLink;
    }

    public void setAdditionalImageLink(String additionalImageLink) {
        this.additionalImageLink = additionalImageLink;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getGoogleProductCategory() {
        return googleProductCategory;
    }

    public void setGoogleProductCategory(String googleProductCategory) {
        this.googleProductCategory = googleProductCategory;
    }

    public String getAvailability() {
        return availability;
    }

    public void setAvailability(String availability) {
        this.availability = availability;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getCatalogId() {
        return catalogId;
    }

    public void setCatalogId(String catalogId) {
        this.catalogId = catalogId;
    }

    public String getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(String advertiserId) {
        this.advertiserId = advertiserId;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id).append("\t")
                .append(imageLink).append("\t")
                .append(additionalImageLink).append("\t")
                .append(title).append("\t")
                .append(description).append("\t")
                .append(googleProductCategory).append("\t")
                .append(availability).append("\t")
                .append(timestamp).append("\t")
                .append(catalogId).append("\t")
                .append(advertiserId);
        return stringBuilder.toString();
    }
}
