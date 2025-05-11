/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.flink.row.FlinkAsFlussRow;
import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

/** Default implementation of RowDataConverter for RowData. */
public class RowSerializationSchema implements FlussSerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;
    private final boolean isAppendOnly;
    private final boolean ignoreDelete;

    private transient FlinkAsFlussRow converter;

    public RowSerializationSchema(boolean isAppendOnly, boolean ignoreDelete) {
        this.isAppendOnly = isAppendOnly;
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.converter = new FlinkAsFlussRow();
    }

    @Override
    public RowWithOp serialize(RowData value) throws Exception {
        InternalRow row = converter.replace(value);
        OperationType opType = toOperationType(value.getRowKind());
        if (opType == null) {
            return null;
        } else {
            return new RowWithOp(row, opType);
        }
    }

    @Nullable
    private OperationType toOperationType(RowKind rowKind) {
        if (ignoreDelete) {
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                return null;
            }
        }
        if (isAppendOnly) {
            if (rowKind == RowKind.INSERT) {
                return OperationType.APPEND;
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported row kind: " + rowKind + " in append-only mode");
            }
        } else {
            switch (rowKind) {
                case INSERT:
                case UPDATE_AFTER:
                    return OperationType.UPSERT;
                case DELETE:
                case UPDATE_BEFORE:
                    return OperationType.DELETE;
                default:
                    throw new UnsupportedOperationException("Unsupported row kind: " + rowKind);
            }
        }
    }
}
