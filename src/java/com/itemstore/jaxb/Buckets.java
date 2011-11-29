package com.itemstore.jaxb;

import com.itemstore.beans.entities.Bucket;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author Ashwani Priyedarshi
 * Class Buckets used to generate the XML for the Buckets
 */
@XmlRootElement(name = "buckets")
public class Buckets {

    private List<Bucket> bucket;

    public Buckets() {
    }

    public Buckets(List<Bucket> bucket) {
        this.bucket = bucket;
    }

    @XmlElement(name = "bucket")
    public List<Bucket> getBuckets() {
        return bucket;
    }

    public void setBuckets(List<Bucket> bucket) {
        this.bucket = bucket;
    }
}
