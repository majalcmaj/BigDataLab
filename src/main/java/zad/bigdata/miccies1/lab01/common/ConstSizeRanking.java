package zad.bigdata.miccies1.lab01.common;

import lombok.Getter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by mc on 04.03.17.
 */
public class ConstSizeRanking implements Writable, Iterable<ConstSizeRanking.Item>{

    public ConstSizeRanking() {
        this.items = new ArrayList<Item>();
    }
    public ConstSizeRanking(int maxSize) {
        this.maxSize = maxSize;
        this.items = new ArrayList<Item>(maxSize + 1);
    }

    public boolean insertItem(int key, String value) {
        boolean itemAdded = false;
        if(items.size() < maxSize) {
            items.add(new Item(key, value));
            itemAdded = true;
        } else {
            if(key > items.get(items.size() - 1).getKey()) {
                items.add(new Item(key, value));
                itemAdded = true;
            }
        }
        if(itemAdded) {
            Collections.sort(items);
            if(items.size() > maxSize) {
                items.remove(maxSize);
            }
        }
        return itemAdded;
    }

    public Item getItem(int index) {
        return items.get(index);
    }

    public List<Item> getItems() {
        return new ArrayList<Item>(this.items);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(maxSize);
        for(Item item : items) {
            item.write(dataOutput);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.maxSize = dataInput.readInt();
        for(int i = 0; i < maxSize; i ++) {
            items.add(new Item(dataInput));
        }
    }

    public int size() {
        return items.size();
    }

    public Iterator<Item> iterator() {
        return items.iterator();
    }

    @Getter
    private int maxSize = 0;
    private List<Item> items = new ArrayList<Item>();

    public class Item implements Comparable<Item>, Writable{
        @Getter
        private int key;
        @Getter
        private String value;

        public Item(int key, String value) {
            super();
            this.key = key;
            this.value = value;
        }

        public Item(DataInput din) throws IOException{
            this.readFields(din);
        }

        public int compareTo(Item o) {
            return o.getKey() - key;
        }

        public void readFields(DataInput din) throws IOException {
            key = din.readInt();
            value = din.readUTF();
        }

        public void write(DataOutput dout) throws IOException {
            dout.writeInt(key);
            dout.writeUTF(value);
        }

        @Override
        public String toString() {
            return String.format("Item Key: %d value: '%s'", key, value);
        }
    }
}
