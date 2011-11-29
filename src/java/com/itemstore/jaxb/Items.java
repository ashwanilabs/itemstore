package com.itemstore.jaxb;

import com.itemstore.beans.entities.Item;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author Ashwani Priyedarshi
 * Class Items used to generate the XML for the Items
 */
@XmlRootElement(name="items")
public class Items {

    private List<Item> item;

    public Items() {
    }

    public Items(List<Item> item) {
        this.item = item;
    }

    @XmlElement(name="item")
    public List<Item> getitem() {
        return item;
    }

    public void setitem(List<Item> item) {
        this.item = item;
    }
}
