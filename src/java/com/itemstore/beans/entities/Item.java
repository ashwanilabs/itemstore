package com.itemstore.beans.entities;

import com.itemstore.types.ItemStoreObject;
import com.itemstore.types.Log;
import java.io.Serializable;
import java.util.Date;
import java.util.GregorianCalendar;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.IdClass;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Temporal;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import org.apache.commons.lang.SerializationUtils;

/**
 * @author Ashwani Priyedarshi
 * Entity Class Item
 */
@XmlType(name = "Item")
@Entity(name = "ITEM")
@IdClass(Item.ItemId.class)
public class Item implements Serializable, Comparable<Item> {

    private static final long serialVersionUID = 1L;
    @Id
    @Column(name = "ITEM_NM", nullable = false, length = 32)
    private String itemnm;
    @Id
    @Column(name = "BUCKET_NM", nullable = false, length = 32)
    private String bucketnm;
    @Id
    @Column(name = "USER_ID", nullable = false, length = 32)
    private String userid;
    @Column(name = "TYPE", length = 32)
    private String type;
    @Column(name = "URI", length = 128)
    private String uri;
    @Column(name = "ORIGIN", length = 32)
    private String origin;
    @Column(name = "SERVERS", length = 128)
    private String servers;
    @Column(name = "LAST_MODIFIED")
    @Temporal(javax.persistence.TemporalType.DATE)
    private Date lastModified;
    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "CONTENT")
    private byte[] blob;

    public Item() {
    }

    public Item(String itemnm, String bucketnm, String userid, String type, String uri, String origin, String servers) {
        this.itemnm = itemnm;
        this.bucketnm = bucketnm;
        this.userid = userid;
        this.type = type;
        this.uri = uri;
        this.origin = origin;
        this.servers = servers;
        this.lastModified = new GregorianCalendar().getTime();
    }

    public Item(String itemnm, String bucketnm, String userid, String type, String uri, String origin, String servers, Date lastModified) {
        this.itemnm = itemnm;
        this.bucketnm = bucketnm;
        this.userid = userid;
        this.type = type;
        this.uri = uri;
        this.origin = origin;
        this.servers = servers;
        this.lastModified = lastModified;
    }

    public Item(String itemString) {
        String[] s = itemString.split(",");
        this.itemnm = s[2];
        this.bucketnm = s[1];
        this.userid = s[0];
        this.type = s[6];
        this.uri = s[7];
        this.origin = s[4];
        this.servers = s[5];
        this.lastModified = new Date(Long.parseLong(s[3]));
    }

    @XmlElement
    public String getItemnm() {
        return itemnm;
    }

    @XmlTransient
    public String getBucketnm() {
        return bucketnm;
    }

    @XmlElement
    public Date getLastModified() {
        return lastModified;
    }

    @XmlElement
    public String getType() {
        return type;
    }

    @XmlElement
    public String getUri() {
        return uri;
    }

    public void setBucketnm(String bucketnm) {
        this.bucketnm = bucketnm;
    }

    public void setItemnm(String itemnm) {
        this.itemnm = itemnm;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @XmlTransient
    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    @XmlTransient
    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    @XmlTransient
    public byte[] getBlob() {
        return blob;
    }

    public void setBlob(byte[] blob) {
        this.blob = blob;
    }

    @XmlTransient
    public ItemStoreObject getItem() {
        return (ItemStoreObject) SerializationUtils.deserialize(blob);
    }

    public void setItem(ItemStoreObject item) {
        this.blob = SerializationUtils.serialize(item);
    }

    public void merge(Item item) {
        if (this.lastModified.before(item.lastModified)
                || (this.lastModified.getTime() == item.lastModified.getTime() && this.origin.compareTo(item.origin) < 0)) {
            this.itemnm = item.itemnm;
            this.bucketnm = item.bucketnm;
            this.userid = item.userid;
            this.uri = item.uri;
            this.type = item.type;
            this.servers = item.servers;
            this.origin = item.origin;
            this.lastModified = item.lastModified;
            //BLOB
            if (item.blob != null) {
                this.blob = item.blob;
            }
        }
    }

    public void merge2(Item item, Log log) {
        if (this.lastModified.before(item.lastModified)
                || (this.lastModified.getTime() == item.lastModified.getTime() && this.origin.compareTo(item.origin) < 0)) {
            this.itemnm = item.itemnm;
            this.bucketnm = item.bucketnm;
            this.userid = item.userid;
            this.uri = item.uri;
            this.type = item.type;
            this.servers = item.servers;
            this.origin = item.origin;
            this.lastModified = item.lastModified;
            ItemStoreObject itemObj = getItem();
            itemObj.applyLog(log);
            setItem(itemObj);
        }
    }

    public String getString() {
        return userid + "," + bucketnm + "," + itemnm + "," + lastModified.getTime() + "," + origin + "," + servers + "," + type + "," + uri;
    }

    @Override
    public String toString() {
        return itemnm + ":" + bucketnm + ":" + userid;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Item other = (Item) obj;
        if ((this.itemnm == null) ? (other.itemnm != null) : !this.itemnm.equals(other.itemnm)) {
            return false;
        }
        if ((this.bucketnm == null) ? (other.bucketnm != null) : !this.bucketnm.equals(other.bucketnm)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + (this.itemnm != null ? this.itemnm.hashCode() : 0);
        hash = 53 * hash + (this.bucketnm != null ? this.bucketnm.hashCode() : 0);
        return hash;
    }

    @Override
    public int compareTo(Item o) {
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

    public static class ItemId implements Serializable {

        String itemnm;
        String bucketnm;
        String userid;

        public ItemId() {
        }

        public ItemId(String itemnm, String bucketnm, String userid) {
            this.itemnm = itemnm;
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
            final ItemId other = (ItemId) obj;
            if ((this.itemnm == null) ? (other.itemnm != null) : !this.itemnm.equals(other.itemnm)) {
                return false;
            }
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
            int hash = 3;
            hash = 19 * hash + (this.itemnm != null ? this.itemnm.hashCode() : 0);
            hash = 19 * hash + (this.bucketnm != null ? this.bucketnm.hashCode() : 0);
            hash = 19 * hash + (this.userid != null ? this.userid.hashCode() : 0);
            return hash;
        }

        @Override
        public String toString() {
            return "ItemId{" + "itemnm=" + itemnm + "bucketnm=" + bucketnm + "userid=" + userid + '}';
        }
    }
}
