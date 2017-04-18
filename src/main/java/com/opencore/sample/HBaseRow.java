package com.opencore.sample;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A container wrapper for a row in HBase
 */
public class HBaseRow implements Comparable<HBaseRow>, Serializable {

    private String columnFamily;
    private byte[] rowKey;
    private Map<String, byte[]> columns;

    public HBaseRow() {
        columns = new HashMap<String, byte[]>();
    }

    public HBaseRow(String columnFamily, byte[] rowKey, Map<String, byte[]> columns) {
        this.columnFamily = columnFamily;
        this.rowKey = rowKey;
        this.columns = columns;
    }

    public long getSize() {
        long size = rowKey.length;
        for (byte[] colVal : columns.values()) {
            size += colVal.length;
        }
        return size;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, byte[]> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, byte[]> columns) {
        this.columns = columns;
    }

    public void addColumn(String col, byte[] val) {
        this.columns.put(col,val);
    }

    public int compareTo(HBaseRow o) {
        byte[] r1 = getRowKey();
        byte[] r2 = o.getRowKey();
        for (int i = 0; i < Math.min(r1.length, r2.length); i++) {
            if (r1[i] != r2[i]) {
                return (r1[i] & 0xff) - (r2[i] & 0xff);
            }
        }
        return r1.length - r2.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HBaseRow hBaseRow = (HBaseRow) o;

        if (!columnFamily.equals(hBaseRow.columnFamily)) return false;
        if (!Arrays.equals(rowKey, hBaseRow.rowKey)) return false;
        return columns.equals(hBaseRow.columns);

    }

    @Override
    public int hashCode() {
        int result = columnFamily.hashCode();
        result = 31 * result + Arrays.hashCode(rowKey);
        result = 31 * result + columns.hashCode();
        return result;
    }
}
