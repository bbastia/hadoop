package org.upgrad.dataType;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair(Text first, Text second) {
        setFirst(first);
        setSecond(second);
    }

    public TextPair() {
        setFirst(new Text());
        setSecond(new Text());
    }

    public TextPair(String first, String second) {
        setFirst(new Text(first));
        setSecond(new Text(second));
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(Text second) {
    	this.second=second;
    }

    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public String toString() {
        return first + " " + second;
    }

    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);

        if (cmp != 0) {
            return cmp;
        }

        return second.compareTo(tp.second);
    }

    @Override
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof TextPair)
        {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }



}
