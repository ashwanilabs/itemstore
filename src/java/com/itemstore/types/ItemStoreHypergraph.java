package com.itemstore.types;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Class ItemStoreHypergraph is a CRDT implementation of Hypergraphs.
 * @author Ashwani Priyedarshi
 */
public class ItemStoreHypergraph extends ItemStoreObject {

    private static final long serialVersionUID = 1L;
    //payload set VA, VR, EA, ER
    private Map<String, Object> VA;
    private Set<String> VR;
    private Map<String, Object> EA;
    private Set<String> ER;

    public ItemStoreHypergraph(String itemnm, String bucketnm, String userid, Serializable itemObj, boolean atSource) {
        super(itemnm, bucketnm, userid, "hg", itemObj, atSource);
        this.VA = new HashMap<String, Object>();
        this.VR = new HashSet<String>();
        this.EA = new HashMap<String, Object>();
        this.ER = new HashSet<String>();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        atSource = false;
    }

    public Map<String, Object> getEA() {
        return EA;
    }

    public void setEA(Map<String, Object> EA) {
        this.EA = EA;
    }

    public Set<String> getER() {
        return ER;
    }

    public void setER(Set<String> ER) {
        this.ER = ER;
    }

    public Map<String, Object> getVA() {
        return VA;
    }

    public void setVA(Map<String, Object> VA) {
        this.VA = VA;
    }

    public Set<String> getVR() {
        return VR;
    }

    public void setVR(Set<String> VR) {
        this.VR = VR;
    }

    

    //lookup (vertex v) : boolean b
    public boolean verLookup(String v) {
        return VA.containsKey(v) && !VR.contains(v);
    }

    //lookup (edge (u, v)) : boolean b
    public boolean edgeLookup(String u, String v) {
        return verLookup(u) && verLookup(v) && EA.containsKey(u + ":" + v) && !ER.contains(u + ":" + v);
    }

    private boolean versLookup(Set<String> e) {
        for (String v : e) {
            if (!verLookup(v)) {
                return false;
            }
        }
        return true;
    }

    //lookup (edge (u, v)) : boolean b
    public boolean edgeLookup(SortedSet<String> u, SortedSet<String> v) {
        return versLookup(u) && versLookup(v) && EA.containsKey(flat(u) + ":" + flat(v)) && !ER.contains(flat(u) + ":" + flat(v));
    }

    //update addVertex (vertex w)
    public void addVer(String w, Serializable obj) {
        VA.put(w, obj);
    }

    //update addEdge (vertex u, vertex v)
    public void addEgde(SortedSet<String> u, SortedSet<String> v, Serializable obj) {
        if (!atSource || (versLookup(u) && versLookup(v))) {
            EA.put(flat(u) + ":" + flat(v), obj);
        }
    }

    //update addEdge (vertex u, vertex v)
    public void addEgde(String u, String v, Object obj) {
        if (!atSource || (verLookup(u) && verLookup(v))) {
            EA.put(u + ":" + v, obj);
        }
    }

    private boolean isIsolated(String w) {
        for (String e : EA.keySet()) {
            if (e.indexOf(w) != -1 && (e.indexOf("," + w) != -1
                    || e.indexOf(w + ",") != -1 || e.indexOf(":" + w) != -1
                    || e.indexOf(w + ":") != -1)) {
                for (String er : ER) {
                    if (er.indexOf(":" + w) != -1 || er.indexOf(w + ":") != -1) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    //update removeVertex (vertex w)
    public void removeVer(String w) {
        if (!atSource || (verLookup(w) && isIsolated(w))) {
            if (VA.containsKey(w)) {
                VR.add(w);
            }
        }
    }

    //update removeEdge (edge (u, v))
    public void removeEdge(SortedSet<String> u, SortedSet<String> v) {
        if (!atSource || edgeLookup(u, v)) {
            if (EA.containsKey(flat(u) + ":" + flat(v))) {
                ER.add(flat(u) + ":" + flat(v));
            }
        }
    }

    //update removeEdge (edge (u, v))
    public void removeEdge(String u, String v) {
        if (!atSource || edgeLookup(u, v)) {
            if (EA.containsKey(u + ":" + v)) {
                ER.add(u + ":" + v);
            }
        }
    }

    public void removeEdgeFrom(SortedSet<String> w) {
        if (!atSource || versLookup(w)) {
            String edge = getEdgeFrom(flat(w));
            if (edge != null) {
                ER.add(edge);
            }
        }
    }

    public Object getVer(String w) {
        if (verLookup(w)) {
            return VA.get(w);
        } else {
            return null;
        }
    }

    public Object getEdge(SortedSet<String> u, SortedSet<String> v) {
        if (edgeLookup(u, v)) {
            return EA.get(flat(u) + ":" + flat(v));
        } else {
            return null;
        }
    }

    private String flat(SortedSet<String> u) {
        String str = "";
        for (String s : u) {
            str = str + s + ",";
        }
        return str.substring(0, str.length() - 1);
    }

    private String getEdgeFrom(String w) {
        for (String e : EA.keySet()) {
            if (e.startsWith(w + ":")) {
                return e;
            }
        }
        return null;
    }

    public void merge(ItemStoreHypergraph HG){
        VA.putAll(HG.VA);
        VR.addAll(HG.VR);
        EA.putAll(HG.EA);
        ER.addAll(HG.ER);
    }

    @Override
    public void applyLog(Log log) {
        System.out.println("ITEMSTORE::ItemStoreHypergraph applyLog()");
        for (Operation op : log.getOps()) {
            if (op.methodNm.equals("addVer")) {
                addVer(op.params.get("v"), op.obj);
            } else if (op.methodNm.equals("addEdge")) {
                addEgde(op.params.get("u"), op.params.get("v"), op.obj);
            } else if (op.methodNm.equals("removeVer")) {
                removeVer(op.params.get("v"));
            } else if (op.methodNm.equals("removeEdge")) {
                removeEdge(op.params.get("u"), op.params.get("v"));
            } else if (op.methodNm.equals("setItemObj")) {
                setItemObj(op.params.get("itemObj"));
            }
        }
    }
}
