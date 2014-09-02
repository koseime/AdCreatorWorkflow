package com.kosei.adcreatorworkflow.hadoop;


/**
 * Created by lanceriedel on 7/18/14.
 *
 * Tenn Cool Flow Ladies Long Sleeved Cycle Jersey	http://www.example.com/clothing/sports/product?id=CLO1029384&src=gshopping&popup=false	A ladies' cycling jersey designed for.	CLO-1029384	new	33.99 USD	available for order	http://images.example.com/CLO-1029384.jpg	US::Standard Free Shipping:0 USD		5060155240282				Sporting Goods > Outdoor Recreation > Cycling > Bicycle Clothing > Bicycle Jerseys		Black/Red/White	M	Female	Adult		25.49 USD	2011-03-01T16:00-08:00/2011-03-03T16:00-08:00
 */
public class GoogleProductItem {
    public static int NUM_PRODUCT_ITEM_ENTRIES = 23;

    private String title;
    private String link;
    private String description;
    private String id;
    private String condition;
    private String price;
    private String availability;
    private String imageLink;
    private String shipping;
    private String weight;
    private String gtin;
    private String brand;
    private String mpn;
    private String googleProductCategory;
    private String productType;
    private String additionalImageLink;
    private String color;
    private String  size;
    private String gender;
    private String ageGroup;
    private String itemGroupId;
    private String salePrice;
    private String salePriceEffectiveDate;

    public GoogleProductItem() {}

    public static GoogleProductItem fromParse(String in) {
        String s[] = in.split("\t");
        GoogleProductItem i = new GoogleProductItem();
        i.setTitle(s[0]);
        i.setLink(s[1]);
        i.setDescription(s[2]);
        i.setId(s[3]);
        i.setCondition(s[4]);
        i.setPrice(s[5]);
        i.setAvailability(s[6]);
        i.setImageLink(s[7]);
        i.setShipping(s[8]);
        i.setWeight(s[9]);
        i.setGtin(s[10]);
        i.setBrand(s[11]);
        i.setMpn(s[12]);
        i.setGoogleProductCategory(s[13]);
        i.setProductType(s[14]);
        i.setAdditionalImageLink(s[15]);
        i.setColor(s[16]);
        i.setSize(s[17]);
        i.setGender(s[18]);
        i.setAgeGroup(s[19]);
        i.setItemGroupId(s[20]);
        i.setSalePrice(s[21]);
        i.setSalePriceEffectiveDate(s[22]);

        return i;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getAvailability() {
        return availability;
    }

    public void setAvailability(String availability) {
        this.availability = availability;
    }

    public String getImageLink() {
        return imageLink;
    }

    public void setImageLink(String imageLink) {
        this.imageLink = imageLink;
    }

    public String getShipping() {
        return shipping;
    }

    public void setShipping(String shipping) {
        this.shipping = shipping;
    }

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    public String getGtin() {
        return gtin;
    }

    public void setGtin(String gtin) {
        this.gtin = gtin;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getMpn() {
        return mpn;
    }

    public void setMpn(String mpn) {
        this.mpn = mpn;
    }

    public String getGoogleProductCategory() {
        return googleProductCategory;
    }

    public void setGoogleProductCategory(String googleProductCategory) {
        this.googleProductCategory = googleProductCategory;
    }

    public String getProductType() {
        return productType;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public String getAdditionalImageLink() {
        return additionalImageLink;
    }

    public void setAdditionalImageLink(String additionalImageLink) {
        this.additionalImageLink = additionalImageLink;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getAgeGroup() {
        return ageGroup;
    }

    public void setAgeGroup(String ageGroup) {
        this.ageGroup = ageGroup;
    }

    public String getItemGroupId() {
        return itemGroupId;
    }

    public void setItemGroupId(String itemGroupId) {
        this.itemGroupId = itemGroupId;
    }

    public String getSalePrice() {
        return salePrice;
    }

    public void setSalePrice(String salePrice) {
        this.salePrice = salePrice;
    }

    public String getSalePriceEffectiveDate() {
        return salePriceEffectiveDate;
    }

    public void setSalePriceEffectiveDate(String salePriceEffectiveDate) {
        this.salePriceEffectiveDate = salePriceEffectiveDate;
    }

    public static String getSchema() {
        return schema;
    }

    //
    private final static String schema ="title	link	description	id	condition	price	availability	image link	shipping	shipping weight	gtin	brand	mpn	google product category	product type	additional image link	color	size	gender	age group	item group id	sale price	sale price effective date";


    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer. append((title==null)?"" : title);
        buffer.append("\t");
        buffer. append((link==null)?"" : link);
        buffer.append("\t");
        buffer. append((description==null)?"" : description);
        buffer.append("\t");
        buffer. append((id==null)?"" : id);
        buffer.append("\t");
        buffer. append((condition==null)?"" : condition);
        buffer.append("\t");
        buffer. append((price==null)?"" : price);
        buffer.append("\t");
        buffer. append((availability==null)?"" : availability);
        buffer.append("\t");
        buffer. append((imageLink==null)?"" : imageLink);
        buffer.append("\t");
        buffer. append((shipping==null)?"" : shipping);
        buffer.append("\t");
        buffer. append((weight==null)?"" : weight);
        buffer.append("\t");
        buffer. append((gtin==null)?"" : gtin);
        buffer.append("\t");
        buffer. append((brand==null)?"" : brand);
        buffer.append("\t");
        buffer. append((mpn==null)?"" : mpn);
        buffer.append("\t");
        buffer. append((googleProductCategory==null)?"" : googleProductCategory);
        buffer.append("\t");
        buffer. append((productType==null)?"" : productType);
        buffer.append("\t");
        buffer. append((additionalImageLink==null)?"" : additionalImageLink);
        buffer.append("\t");
        buffer. append((color==null)?"" : color);
        buffer.append("\t");
        buffer. append((size==null)?"" : size);
        buffer.append("\t");
        buffer. append((gender==null)?"" : gender);
        buffer.append("\t");
        buffer. append((ageGroup==null)?"" : ageGroup);
        buffer.append("\t");
        buffer. append((itemGroupId==null)?"" : itemGroupId);
        buffer.append("\t");
        buffer. append((salePrice==null)?"" : salePrice);
        buffer.append("\t");
        buffer. append((salePriceEffectiveDate==null)?"" : salePriceEffectiveDate);
        buffer.append("\t");
        buffer.append("|");

        return buffer.toString();

    }
}
