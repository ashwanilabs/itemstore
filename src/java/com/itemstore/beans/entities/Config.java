package com.itemstore.beans.entities;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 *
 * @author ashwanilabs
 */
@Entity(name = "CONFIG")
public class Config implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @Column(name = "PARAM_NM", nullable = false, length = 32)
    private String paramnm;
    @Column(name = "PARAM_VAL", nullable = false, length = 256)
    private String paramval;

    public String getParamnm() {
        return paramnm;
    }

    public void setParamnm(String paramnm) {
        this.paramnm = paramnm;
    }

    public String getParamval() {
        return paramval;
    }

    public void setParamval(String paramval) {
        this.paramval = paramval;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Config other = (Config) obj;
        if ((this.paramnm == null) ? (other.paramnm != null) : !this.paramnm.equals(other.paramnm)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + (this.paramnm != null ? this.paramnm.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "Config{" + "paramnm=" + paramnm + '}';
    }
}
