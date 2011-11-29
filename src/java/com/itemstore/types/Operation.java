package com.itemstore.types;

import java.io.Serializable;
import java.util.Map;

/**
 * Class Operation is used to define the structure of the operations within a
 * Log defined in a CRDT data type
 * @author Ashwani Priyedarshi
 */
public class Operation implements Serializable{
    
    private static final long serialVersionUID = 1L;

    public String methodNm;
    public Map<String, String> params;
    public Serializable obj;

    public Operation(String methodNm, Map<String, String> params, Serializable obj) {
        this.methodNm = methodNm;
        this.params = params;
        this.obj = obj;
    }
}
