package com.itemstore.types;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Class Log is used to keep the logs of operations performed on an CRDT
 * data type implemented in the system
 * @author Ashwani Priyedarshi
 */
public class Log implements Serializable {

    private static final long serialVersionUID = 1L;
    private String method;
    private ArrayList<Operation> ops;

    public Log(String method, ArrayList<Operation> ops) {
        this.method = method;
        this.ops = ops;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public ArrayList<Operation> getOps() {
        return ops;
    }

    public void setOps(ArrayList<Operation> ops) {
        this.ops = ops;
    }
}
