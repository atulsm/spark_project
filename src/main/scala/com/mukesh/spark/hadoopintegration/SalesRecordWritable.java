package com.mukesh.spark.hadoopintegration;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by mukesh on 7/1/15.
 */
public class SalesRecordWritable implements WritableComparable<SalesRecordWritable> , Serializable {
    private String transactionId;
    private String customerId;
    private String itemId;
    private double itemValue;

    public SalesRecordWritable(String transactionId, String customerId, String itemId, double itemValue) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.itemId = itemId;
        this.itemValue = itemValue;
    }

    public SalesRecordWritable() {
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public double getItemValue() {
        return itemValue;
    }

    public void setItemValue(double itemValue) {
        this.itemValue = itemValue;
    }

    @Override
    public int compareTo(SalesRecordWritable o) {
        return this.transactionId.compareTo(o.transactionId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(transactionId);
        out.writeUTF(customerId);
        out.writeUTF(itemId);
        out.writeDouble(itemValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        transactionId = in.readUTF();
        customerId = in.readUTF();
        itemId = in.readUTF();
        itemValue = in.readDouble();
    }
}
