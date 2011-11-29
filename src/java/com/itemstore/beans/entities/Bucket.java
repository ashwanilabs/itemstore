package com.itemstore.beans.entities;

import java.io.Serializable;
import java.util.Date;
import java.util.GregorianCalendar;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Column;
import javax.persistence.IdClass;
import javax.persistence.Temporal;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author Ashwani Priyedarshi
 * Entity Class Bucket
 */
@XmlType(name = "Bucket")
@Entity(name = "BUCKET")
@IdClass(Bucket.BucketId.class)
public class Bucket implements Serializable, Comparable<Bucket> {

    private static final long serialVersionUID = 1L;
    @Id
    @Column(name = "BUCKET_NM", nullable = false, length = 32)
    private String bucketnm;
    @Id
    @Column(name = "USER_ID", nullable = false, length = 32)
    private String userid;
    @Column(name = "URI", length = 128)
    private String uri;
    @Column(name = "ITEM_NOS")
    private int item_nos;
    @Column(name = "ORIGIN", length = 32)
    private String origin;
    @Column(name = "LAST_MODIFIED")
    @Temporal(javax.persistence.TemporalType.DATE)
    private Date lastModified;

    public Bucket() {
    }

    public Bucket(String bucketnm, String uri, int item_nos, String userid, String origin) {
        this.bucketnm = bucketnm;
        this.uri = uri;
        this.item_nos = item_nos;
        this.userid = userid;
        this.origin = origin;
        this.lastModified = new GregorianCalendar().getTime();
    }

    public Bucket(String bucketString) {
        String[] s = bucketString.split(",");
        this.bucketnm = s[1];
        this.userid = s[0];
        this.item_nos = Integer.parseInt(s[2]);
        this.uri = s[5];
        this.origin = s[4];
        this.lastModified = new Date(Long.parseLong(s[3]));
    }

    @XmlElement
    public String getBucketnm() {
        return bucketnm;
    }

    public void setBucketnm(String bucketnm) {
        this.bucketnm = bucketnm;
    }

    @XmlElement
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @XmlElement
    public int getItem_nos() {
        return item_nos;
    }

    public void setItem_nos(int item_nos) {
        this.item_nos = item_nos;
    }

    @XmlElement
    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    @XmlElement
    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public void merge(Bucket bucket) {
        if (this.lastModified.before(bucket.lastModified)
                || (this.lastModified.getTime() == bucket.lastModified.getTime() && this.origin.compareTo(bucket.origin) < 0)) {
            this.bucketnm = bucket.bucketnm;
            this.item_nos = bucket.item_nos;
            this.lastModified = bucket.lastModified;
            this.uri = bucket.uri;
            this.userid = bucket.userid;
            this.origin = bucket.origin;
        }
    }

    public String getString() {
        return userid + "," + bucketnm + "," + item_nos + "," + lastModified.getTime() + "," + origin + "," + uri;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Bucket other = (Bucket) obj;
        if ((this.bucketnm == null) ? (other.bucketnm != null) : !this.bucketnm.equals(other.bucketnm)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 71 * hash + (this.bucketnm != null ? this.bucketnm.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return bucketnm + ":" + userid;
    }

    @Override
    public int compareTo(Bucket o) {
        if (o == null) {
            return -1;
        } else if (this.lastModified.before(o.lastModified)) {
            return -1;
        } else if (this.lastModified.after(o.lastModified)) {
            return 1;
        } else if (this.origin.compareTo(o.origin) < 0) {
            return -1;
        } else {
            return 1;
        }
    }

    public static class BucketId implements Serializable {

        String bucketnm;
        String userid;

        public BucketId() {
        }

        public BucketId(String bucketnm, String userid) {
            this.bucketnm = bucketnm;
            this.userid = userid;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final BucketId other = (BucketId) obj;
            if ((this.bucketnm == null) ? (other.bucketnm != null) : !this.bucketnm.equals(other.bucketnm)) {
                return false;
            }
            if ((this.userid == null) ? (other.userid != null) : !this.userid.equals(other.userid)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 53 * hash + (this.bucketnm != null ? this.bucketnm.hashCode() : 0);
            hash = 53 * hash + (this.userid != null ? this.userid.hashCode() : 0);
            return hash;
        }
    }
}
