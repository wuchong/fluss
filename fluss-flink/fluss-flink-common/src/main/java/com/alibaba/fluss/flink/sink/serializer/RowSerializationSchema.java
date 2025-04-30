package com.alibaba.fluss.flink.sink.serializer;

import org.apache.flink.table.data.RowData;

/** Default implementation of RowDataConverter for RowData. */
public class RowSerializationSchema implements FlussSerializationSchema<RowData> {
    @Override
    public void open(InitializationContext context) throws Exception {}

    @Override
    public RowData serialize(RowData value) throws Exception {
        return value;
    }
}
