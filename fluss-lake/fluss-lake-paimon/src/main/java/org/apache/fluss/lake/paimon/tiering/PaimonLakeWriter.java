/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.batch.ArrowRecordBatch;
import org.apache.fluss.lake.batch.RecordBatch;
import org.apache.fluss.lake.paimon.tiering.append.AppendOnlyWriter;
import org.apache.fluss.lake.paimon.tiering.mergetree.MergeTreeWriter;
import org.apache.fluss.lake.paimon.utils.PaimonUtils;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.SupportsRecordBatchWrite;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;

/** Implementation of {@link LakeWriter} for Paimon. */
public class PaimonLakeWriter implements LakeWriter<PaimonWriteResult>, SupportsRecordBatchWrite {

    private final Catalog paimonCatalog;
    private final RecordWriter<?> recordWriter;

    public PaimonLakeWriter(
            PaimonCatalogProvider paimonCatalogProvider, WriterInitContext writerInitContext)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        // Only Fixed Bucket tables (bucket keys non-empty) carry a positive BUCKET in Paimon.
        // Overriding on an Unaware Bucket table (BUCKET = -1) would change its bucket mode.
        // The context always resolves the actual bucket count.
        // 中文解释：仅固定桶模式覆盖 writer 视图的桶数；无分桶键表保持 unaware 模式，不能用正数把其模式改掉。
        // REVIEW [F001][P1]: 历史分区回写沿用自身桶布局，导致 Paimon 主键数据重复。
        // REVIEW [F001][P1]: 历史分区在桶数为 2 时创建，ALTER 后的新普通分区按 4 桶写入并完成 tiering。
        // REVIEW [F001][P1]: 该普通分区过期后，迟到更新被客户端按历史分区的 2 桶路由；这里又将 Paimon writer 的桶数覆盖为 2，而
        // REVIEW [F001][P1]: MergeTreeWriter 仍直接使用历史 TableBucket 的 bucketId 写回原分区。
        // REVIEW [F001][P1]: 最小复现中同一主键原来在 bucket 3/4，更新落到 bucket 1/2，提交成功后该分区 totalBuckets 为 [2,
        // REVIEW [F001][P1]: 4]，行数由 1 变为 2。
        // REVIEW [F001][P1]: 应按每条记录的原始湖分区解析实际桶数，并重新计算湖端 bucketId，不能将历史分区的物理路由直接用于湖写入。
        Integer bucketOverride =
                !writerInitContext.tableInfo().getBucketKeys().isEmpty()
                        ? writerInitContext.bucketCount()
                        : null;
        TablePath lakeTablePath = writerInitContext.tableInfo().getLakeTablePath();
        FileStoreTable fileStoreTable =
                getTable(
                        lakeTablePath,
                        writerInitContext.tableInfo().getTableConfig().isDataLakeAutoCompaction(),
                        bucketOverride);

        List<String> partitionKeys = fileStoreTable.partitionKeys();
        RowType flussRowType = writerInitContext.tableInfo().getRowType();
        boolean historicalPartition =
                HISTORICAL_PARTITION_VALUE.equals(writerInitContext.partition());

        // FIP-27: detect whether the target Paimon table is a clean table (only user columns) or a
        // legacy table (carrying the three Fluss system columns). Writers emit system columns only
        // for legacy tables.
        boolean paimonIncludingSystemColumns = PaimonUtils.isLegacyTable(fileStoreTable.rowType());

        this.recordWriter =
                fileStoreTable.primaryKeys().isEmpty()
                        ? new AppendOnlyWriter(
                                fileStoreTable,
                                writerInitContext.tableBucket(),
                                writerInitContext.partition(),
                                partitionKeys,
                                flussRowType,
                                paimonIncludingSystemColumns,
                                historicalPartition)
                        : new MergeTreeWriter(
                                fileStoreTable,
                                writerInitContext.tableBucket(),
                                writerInitContext.partition(),
                                partitionKeys,
                                flussRowType,
                                writerInitContext.ioTmpDirs(),
                                paimonIncludingSystemColumns,
                                historicalPartition);
    }

    @Override
    public void write(LogRecord record) throws IOException {
        try {
            recordWriter.write(record);
        } catch (Exception e) {
            throw new IOException("Failed to write Fluss record to Paimon.", e);
        }
    }

    @Override
    public void write(RecordBatch recordBatch) throws IOException {
        if (!(recordBatch instanceof ArrowRecordBatch)) {
            throw new IllegalArgumentException(
                    "PaimonLakeWriter only supports ArrowRecordBatch, but got "
                            + recordBatch.getClass().getSimpleName());
        }
        if (!(recordWriter instanceof AppendOnlyWriter)) {
            throw new IllegalStateException(
                    "Arrow record batch writing is only supported for append-only tables.");
        }
        try {
            ((AppendOnlyWriter) recordWriter)
                    .writeArrowBatch(((ArrowRecordBatch) recordBatch).getArrowBatchData());
        } catch (Exception e) {
            throw new IOException("Failed to write Arrow record batch to Paimon.", e);
        }
    }

    @Override
    public PaimonWriteResult complete() throws IOException {
        try {
            return new PaimonWriteResult(recordWriter.complete());
        } catch (Exception e) {
            throw new IOException("Failed to complete Paimon write.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (recordWriter != null) {
                recordWriter.close();
            }
            if (paimonCatalog != null) {
                paimonCatalog.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close PaimonLakeWriter.", e);
        }
    }

    private FileStoreTable getTable(
            TablePath tablePath, boolean isAutoCompaction, @Nullable Integer bucketOverride)
            throws IOException {
        try {
            FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
            if (bucketOverride != null) {
                // copy(Map) rejects BUCKET as immutable, so swap it in via a schema copy,
                // which only rebuilds the in-memory table view.
                // 中文解释：复制的是本 writer 使用的内存 schema 视图，不改 Catalog 默认值，也不在此处重新计算记录的 bucketId。
                Map<String, String> schemaOptions = new HashMap<>(table.schema().options());
                schemaOptions.put(CoreOptions.BUCKET.key(), String.valueOf(bucketOverride));
                table = table.copy(table.schema().copy(schemaOptions));
            }
            Map<String, String> dynamicOptions = new HashMap<>();
            dynamicOptions.put(
                    CoreOptions.WRITE_ONLY.key(),
                    isAutoCompaction ? Boolean.FALSE.toString() : Boolean.TRUE.toString());
            return table.copy(dynamicOptions);
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Paimon.", e);
        }
    }
}
