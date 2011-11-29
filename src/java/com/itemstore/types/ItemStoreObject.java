package com.itemstore.types;

import java.io.IOException;
import java.io.Serializable;

/**
 * Class ItemStoreObject is a parent class which is extended by all the
 * CRDT data types implemented in the system
 * @author Ashwani Priyedarshi
 */
public class ItemStoreObject implements Serializable {

    private static final long serialVersionUID = 1L;
    public String itemnm;
    public String bucketnm;
    public String userid;
    public String type;
    public Serializable itemObj;
    public transient boolean atSource;

    public ItemStoreObject(String itemnm, String bucketnm, String userid, String type, Serializable itemObj, boolean atSource) {
        this.itemnm = itemnm;
        this.bucketnm = bucketnm;
        this.userid = userid;
        this.type = type;
        this.itemObj = itemObj;
        this.atSource = atSource;
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        atSource = false;
    }

    public String getBucketnm() {
        return bucketnm;
    }

    public void setBucketnm(String bucketnm) {
        this.bucketnm = bucketnm;
    }

    public String getItemnm() {
        return itemnm;
    }

    public void setItemnm(String itemnm) {
        this.itemnm = itemnm;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public Serializable getItemObj() {
        return itemObj;
    }

    public void setItemObj(Serializable itemObj) {
        this.itemObj = itemObj;
    }

    public void applyLog(Log log) {
        System.out.println("ITEMSTORE::ItemStoreObject applyLog()");
    }

    public ItemStoreObject dup() {
        if (type.equals("hg")) {
            ItemStoreHypergraph o = (ItemStoreHypergraph) this;
            ItemStoreHypergraph n = new ItemStoreHypergraph(itemnm, bucketnm, userid, itemObj, atSource);
            n.setVA(o.getVA());
            n.setEA(o.getEA());
            n.setVR(o.getVR());
            n.setER(o.getER());
            return n;
        } else {
            return this;
        }
    }

    public void merge(ItemStoreObject itemobj) {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ItemStoreObject other = (ItemStoreObject) obj;
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
        int hash = 5;
        hash = 19 * hash + (this.itemnm != null ? this.itemnm.hashCode() : 0);
        hash = 19 * hash + (this.bucketnm != null ? this.bucketnm.hashCode() : 0);
        hash = 19 * hash + (this.userid != null ? this.userid.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "ItemStoreObject{" + "itemnm=" + itemnm + "bucketnm=" + bucketnm + "userid=" + userid + '}';
    }
}
