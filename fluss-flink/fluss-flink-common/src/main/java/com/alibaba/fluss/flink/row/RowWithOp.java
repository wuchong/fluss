/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.row.InternalRow;

import org.apache.flink.types.RowKind;

import java.util.Objects;

public class RowWithOp<T> {
    private InternalRow internalRow;
    private RowKind rowKind;

    public RowWithOp(InternalRow internalRow) {
        this.internalRow = internalRow;
    }

    public RowWithOp(InternalRow internalRow, RowKind operationType) {
        this.internalRow = internalRow;
        this.rowKind = operationType;
    }

    public InternalRow getInternalRow() {
        return internalRow;
    }

    public void setInternalRow(InternalRow internalRow) {
        this.internalRow = internalRow;
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public void setRowKind(RowKind rowKind) {
        this.rowKind = rowKind;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RowWithOp<?> operation = (RowWithOp<?>) o;
        return Objects.equals(internalRow, operation.internalRow) && rowKind == operation.rowKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(internalRow, rowKind);
    }

    @Override
    public String toString() {
        return "Operation{" + "internalRow=" + internalRow + ", rowKind=" + rowKind + '}';
    }
}
