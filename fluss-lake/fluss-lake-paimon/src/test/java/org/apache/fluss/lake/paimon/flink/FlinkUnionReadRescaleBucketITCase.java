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

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The IT case for union read (lake + fluss log) on a partitioned table whose partitions carry
 * different bucket counts after an ALTER TABLE ... SET ('bucket.num' = N): the partition created
 * before the ALTER keeps its original bucket count while the partition created afterwards uses the
 * new one. Verifies that tiering stamps each partition's files with the partition's actual bucket
 * count and that union read enumerates buckets per partition.
 */
class FlinkUnionReadRescaleBucketITCase extends FlinkUnionReadTestBase {

    private static final int OLD_BUCKET_NUM = 2;
    private static final int NEW_BUCKET_NUM = 4;
    private static final int RECORDS_PER_ROUND = 16;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @Test
    void testUnionReadAcrossPartitionsWithDifferentBucketCounts() throws Exception {
        // 中文解释：让扩容前后的日志分区先入湖，再停止 tiering 并追加 Fluss 数据，验证纯湖、混合和分区过滤读取均返回完整记录。
        String tableName = "rescale_bucket_log_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        createPartitionedLogTable(tablePath, OLD_BUCKET_NUM);

        // "old" partition is created before the ALTER and keeps OLD_BUCKET_NUM buckets
        createPartition(tablePath, "old");
        List<Row> expectedRows = new ArrayList<>(writeRows(tablePath, "old", 0));

        // ALTER bucket.num, which also propagates the new BUCKET to the Paimon table
        alterBucketNum(tablePath);

        // "new" partition is created after the ALTER and uses NEW_BUCKET_NUM buckets
        createPartition(tablePath, "new");
        expectedRows.addAll(writeRows(tablePath, "new", 0));

        Map<String, Integer> bucketCountByPartition = bucketCountByPartitionName(tablePath);
        assertThat(bucketCountByPartition)
                .containsEntry("old", OLD_BUCKET_NUM)
                .containsEntry("new", NEW_BUCKET_NUM);

        // the ALTER must have propagated the new BUCKET to the Paimon schema
        FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        assertThat(paimonTable.options().get(CoreOptions.BUCKET.key()))
                .isEqualTo(String.valueOf(NEW_BUCKET_NUM));

        // start tiering and wait until both partitions are fully synced by their own bucket range
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            waitUntilPartitionBucketsSynced(tablePath, tableId);

            // files of each partition must be stamped with the partition's actual bucket count
            assertThat(totalBucketsOfPartition(tablePath, "old")).containsExactly(OLD_BUCKET_NUM);
            assertThat(totalBucketsOfPartition(tablePath, "new")).containsExactly(NEW_BUCKET_NUM);

            // union read with all data in lake
            List<Row> actual =
                    CollectionUtil.iteratorToList(
                            batchTEnv.executeSql("select * from " + tableName).collect());
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRows);
        } finally {
            jobClient.cancel().get();
        }

        // write more rows after tiering stopped so union read mixes lake splits and fluss log
        // 中文解释：tiering 作业已停止，新增记录只能留在 Fluss，因此这次查询必须真正合并湖快照与 Fluss 尾部。
        expectedRows.addAll(writeRows(tablePath, "old", RECORDS_PER_ROUND));
        expectedRows.addAll(writeRows(tablePath, "new", RECORDS_PER_ROUND));

        List<Row> actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRows);

        // partition filter on the partition with the original bucket count
        List<Row> actualOldPartition =
                CollectionUtil.iteratorToList(
                        batchTEnv
                                .executeSql("select * from " + tableName + " where c = 'old'")
                                .collect());
        List<Row> expectedOldPartition = new ArrayList<>();
        for (Row r : expectedRows) {
            if ("old".equals(r.getField(2))) {
                expectedOldPartition.add(r);
            }
        }
        assertThat(actualOldPartition).containsExactlyInAnyOrderElementsOf(expectedOldPartition);
    }

    @Test
    void testUnionReadPkTableAcrossPartitionsWithDifferentBucketCounts() throws Exception {
        // 中文解释：将不同桶数的主键分区入湖后停止 tiering，再更新已有主键，验证批量联合读取以 Fluss 更新覆盖湖旧值，并正确处理分区过滤。
        String tableName = "rescale_bucket_pk_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        createPartitionedPkTable(tablePath, OLD_BUCKET_NUM);

        // "old" partition keeps OLD_BUCKET_NUM; "new" partition (post-ALTER) uses NEW_BUCKET_NUM
        createPartition(tablePath, "old");
        writeUpsertRows(tablePath, "old", 0);
        alterBucketNum(tablePath);
        createPartition(tablePath, "new");
        writeUpsertRows(tablePath, "new", 0);

        assertThat(bucketCountByPartitionName(tablePath))
                .containsEntry("old", OLD_BUCKET_NUM)
                .containsEntry("new", NEW_BUCKET_NUM);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            waitUntilPartitionBucketsSynced(tablePath, tableId);

            assertThat(totalBucketsOfPartition(tablePath, "old")).containsExactly(OLD_BUCKET_NUM);
            assertThat(totalBucketsOfPartition(tablePath, "new")).containsExactly(NEW_BUCKET_NUM);

            // lake-only read: latest value per key across both partitions, verified by content
            List<Row> expectedSnapshot = new ArrayList<>();
            for (String partition : new String[] {"old", "new"}) {
                for (int i = 0; i < RECORDS_PER_ROUND; i++) {
                    expectedSnapshot.add(Row.of(i, "v" + i, partition));
                }
            }
            List<Row> lakeOnly =
                    CollectionUtil.iteratorToList(
                            batchTEnv.executeSql("select * from " + tableName).collect());
            assertThat(lakeOnly).containsExactlyInAnyOrderElementsOf(expectedSnapshot);
        } finally {
            jobClient.cancel().get();
        }

        // update existing keys after tiering so the read must merge lake snapshot with the fluss
        // log tail (dedup by primary key), on both the old and new bucket-count partitions
        // 中文解释：用相同主键覆盖旧值而不是追加新键，可检查联合读取是否正确消除湖快照中的旧版本。
        List<InternalRow> updates = new ArrayList<>();
        updates.add(row(0, "old-updated", "old"));
        updates.add(row(0, "new-updated", "new"));
        writeRows(tablePath, updates, false);

        // full expected state after the updates: key 0 of each partition carries the new value,
        // every other key keeps its tiered value
        List<Row> expectedMerged = new ArrayList<>();
        expectedMerged.add(Row.of(0, "old-updated", "old"));
        expectedMerged.add(Row.of(0, "new-updated", "new"));
        for (String partition : new String[] {"old", "new"}) {
            for (int i = 1; i < RECORDS_PER_ROUND; i++) {
                expectedMerged.add(Row.of(i, "v" + i, partition));
            }
        }
        List<Row> merged =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());
        assertThat(merged).containsExactlyInAnyOrderElementsOf(expectedMerged);

        // partition filter on the partition that kept the original bucket count
        List<Row> expectedOldOnly = new ArrayList<>();
        for (Row r : expectedMerged) {
            if ("old".equals(r.getField(2))) {
                expectedOldOnly.add(r);
            }
        }
        List<Row> oldOnly =
                CollectionUtil.iteratorToList(
                        batchTEnv
                                .executeSql("select * from " + tableName + " where c = 'old'")
                                .collect());
        assertThat(oldOnly).containsExactlyInAnyOrderElementsOf(expectedOldOnly);
    }

    @Test
    void testStreamUnionReadAcrossPartitionsWithDifferentBucketCounts() throws Exception {
        // 中文解释：完成不同桶数分区的首次入湖后开启流式联合读取，再追加日志，验证湖快照与后续 Fluss 数据均被读出且没有多余记录。
        String tableName = "rescale_bucket_stream_log_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        createPartitionedLogTable(tablePath, OLD_BUCKET_NUM);

        createPartition(tablePath, "old");
        List<Row> expectedRows = new ArrayList<>(writeRows(tablePath, "old", 0));
        alterBucketNum(tablePath);
        createPartition(tablePath, "new");
        expectedRows.addAll(writeRows(tablePath, "new", 0));

        assertThat(bucketCountByPartitionName(tablePath))
                .containsEntry("old", OLD_BUCKET_NUM)
                .containsEntry("new", NEW_BUCKET_NUM);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            waitUntilPartitionBucketsSynced(tablePath, tableId);

            // streaming union read: read lake snapshot then keep streaming the fluss log tail
            CloseableIterator<Row> iterator =
                    streamTEnv
                            .executeSql(
                                    "select * from "
                                            + tableName
                                            + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */")
                            .collect();
            // append more rows to both partitions after starting the stream
            expectedRows.addAll(writeRows(tablePath, "old", RECORDS_PER_ROUND));
            expectedRows.addAll(writeRows(tablePath, "new", RECORDS_PER_ROUND));

            List<String> actual = collectRowsWithTimeout(iterator, expectedRows.size(), true);
            assertThat(actual)
                    .containsExactlyInAnyOrderElementsOf(
                            expectedRows.stream().map(Row::toString).collect(Collectors.toList()));
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testStreamUnionReadPkTableAcrossPartitionsWithDifferentBucketCounts() throws Exception {
        // 中文解释：对不同桶数主键分区从湖快照开始流读，再更新同一主键，验证初始 INSERT 和成对 UPDATE_BEFORE、UPDATE_AFTER 事件均完整。
        String tableName = "rescale_bucket_stream_pk_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        createPartitionedPkTable(tablePath, OLD_BUCKET_NUM);

        createPartition(tablePath, "old");
        writeUpsertRows(tablePath, "old", 0);
        alterBucketNum(tablePath);
        createPartition(tablePath, "new");
        writeUpsertRows(tablePath, "new", 0);

        assertThat(bucketCountByPartitionName(tablePath))
                .containsEntry("old", OLD_BUCKET_NUM)
                .containsEntry("new", NEW_BUCKET_NUM);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            waitUntilPartitionBucketsSynced(tablePath, tableId);
            assertThat(totalBucketsOfPartition(tablePath, "old")).containsExactly(OLD_BUCKET_NUM);
            assertThat(totalBucketsOfPartition(tablePath, "new")).containsExactly(NEW_BUCKET_NUM);

            // streaming union read on the PK table: the snapshot phase emits +I for the latest
            // value of every key across both partitions, then keeps consuming the changelog tail
            CloseableIterator<Row> iterator =
                    streamTEnv
                            .executeSql(
                                    "select * from "
                                            + tableName
                                            + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */")
                            .collect();

            List<String> expectedEvents = new ArrayList<>();
            for (int i = 0; i < RECORDS_PER_ROUND; i++) {
                expectedEvents.add(Row.ofKind(RowKind.INSERT, i, "v" + i, "old").toString());
                expectedEvents.add(Row.ofKind(RowKind.INSERT, i, "v" + i, "new").toString());
            }

            // update one key in each partition after the stream started: the changelog must
            // arrive as -U/+U from the fluss log tail on both the old (2-bucket) and the new
            // (4-bucket) partition, proving the tail is subscribed by per-partition bucket range
            // 中文解释：同时更新新旧分区中的键 0，期望每个分区各产生一对更新事件，验证两种布局下的快照到日志衔接。
            List<InternalRow> updates = new ArrayList<>();
            updates.add(row(0, "old-updated", "old"));
            updates.add(row(0, "new-updated", "new"));
            writeRows(tablePath, updates, false);
            expectedEvents.add(Row.ofKind(RowKind.UPDATE_BEFORE, 0, "v0", "old").toString());
            expectedEvents.add(
                    Row.ofKind(RowKind.UPDATE_AFTER, 0, "old-updated", "old").toString());
            expectedEvents.add(Row.ofKind(RowKind.UPDATE_BEFORE, 0, "v0", "new").toString());
            expectedEvents.add(
                    Row.ofKind(RowKind.UPDATE_AFTER, 0, "new-updated", "new").toString());

            List<String> actual = collectRowsWithTimeout(iterator, expectedEvents.size(), true);
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedEvents);
        } finally {
            jobClient.cancel().get();
        }
    }

    @Test
    void testUnionReadLakeOnlyExpiredPartitionAfterRescale() throws Exception {
        // 中文解释：将旧分区入湖后从 Fluss 删除，验证全表及旧分区过滤查询仍能读取湖端保留的数据，并与新分区结果正确组合。
        String tableName = "rescale_bucket_expired_log_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        createPartitionedLogTable(tablePath, OLD_BUCKET_NUM);

        createPartition(tablePath, "old");
        List<Row> oldRows = writeRows(tablePath, "old", 0);
        alterBucketNum(tablePath);
        createPartition(tablePath, "new");
        List<Row> newRows = writeRows(tablePath, "new", 0);

        assertThat(bucketCountByPartitionName(tablePath))
                .containsEntry("old", OLD_BUCKET_NUM)
                .containsEntry("new", NEW_BUCKET_NUM);

        // tier everything to the lake, then drop the rescaled "old" partition in Fluss so that it
        // survives only in the lake (its files are stamped with OLD_BUCKET_NUM)
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            long tableId = admin.getTableInfo(tablePath).get().getTableId();
            waitUntilPartitionBucketsSynced(tablePath, tableId);
            assertThat(totalBucketsOfPartition(tablePath, "old")).containsExactly(OLD_BUCKET_NUM);
        } finally {
            jobClient.cancel().get();
        }

        // 中文解释：只删除 Fluss 分区，保留已提交的 Paimon 文件，使随后读取必须走湖中独有分区的规划路径。
        admin.dropPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("c", "old")), false)
                .get();
        retry(
                Duration.ofSeconds(60),
                () -> assertThat(admin.listPartitionInfos(tablePath).get()).hasSize(1));

        // union read: the expired "old" partition is served entirely from the lake (with its
        // original bucket count), the "new" partition from Fluss+lake
        List<Row> expected = new ArrayList<>(oldRows);
        expected.addAll(newRows);
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        // partition filter on the lake-only expired partition still returns its data
        List<Row> oldActual =
                CollectionUtil.iteratorToList(
                        batchTEnv
                                .executeSql("select * from " + tableName + " where c = 'old'")
                                .collect());
        assertThat(oldActual).containsExactlyInAnyOrderElementsOf(oldRows);
    }

    private void createPartitionedLogTable(TablePath tablePath, int bucketNum) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .column("c", DataTypes.STRING())
                                        .build())
                        .distributedBy(bucketNum, "a")
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        createTable(tablePath, descriptor);
    }

    private void createPartitionedPkTable(TablePath tablePath, int bucketNum) throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .column("c", DataTypes.STRING())
                                        .primaryKey("a", "c")
                                        .build())
                        .distributedBy(bucketNum, "a")
                        .partitionedBy("c")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .build();
        createTable(tablePath, descriptor);
    }

    private void createPartition(TablePath tablePath, String value) throws Exception {
        admin.createPartition(
                        tablePath, new PartitionSpec(Collections.singletonMap("c", value)), false)
                .get();
    }

    private void alterBucketNum(TablePath tablePath) throws Exception {
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set("bucket.num", String.valueOf(NEW_BUCKET_NUM))),
                        false)
                .get();
    }

    private void writeUpsertRows(TablePath tablePath, String partition, int keyOffset)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = keyOffset; i < keyOffset + RECORDS_PER_ROUND; i++) {
            rows.add(row(i, "v" + i, partition));
        }
        writeRows(tablePath, rows, false);
    }

    private List<Row> writeRows(TablePath tablePath, String partition, int keyOffset)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int i = keyOffset; i < keyOffset + RECORDS_PER_ROUND; i++) {
            rows.add(row(i, "v" + i, partition));
            flinkRows.add(Row.of(i, "v" + i, partition));
        }
        writeRows(tablePath, rows, true);
        return flinkRows;
    }

    private Map<String, Integer> bucketCountByPartitionName(TablePath tablePath) throws Exception {
        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        Map<String, Integer> bucketCountByName = new java.util.HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            bucketCountByName.put(partitionInfo.getPartitionName(), partitionInfo.getBucketCount());
        }
        return bucketCountByName;
    }

    // 中文解释：只等待已有记录的桶出现入湖标记，空桶没有 tiering split，等待它们会造成测试永远无法就绪。
    private void waitUntilPartitionBucketsSynced(TablePath tablePath, long tableId)
            throws Exception {
        // empty buckets never get a tiering split (nothing to tier), so only wait for the lake
        // sync marker on buckets that actually contain data
        BucketOffsetsRetrieverImpl bucketOffsetsRetriever =
                new BucketOffsetsRetrieverImpl(admin, tablePath);
        Set<TableBucket> tableBuckets = new HashSet<>();
        for (PartitionInfo partitionInfo : admin.listPartitionInfos(tablePath).get()) {
            int bucketCount = partitionInfo.getBucketCount();
            List<Integer> buckets = new ArrayList<>();
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                buckets.add(bucket);
            }
            Map<Integer, Long> latestOffsets =
                    bucketOffsetsRetriever.latestOffsets(partitionInfo.getPartitionName(), buckets);
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                Long latestOffset = latestOffsets.get(bucket);
                if (latestOffset != null && latestOffset > 0) {
                    tableBuckets.add(
                            new TableBucket(tableId, partitionInfo.getPartitionId(), bucket));
                }
            }
        }
        waitUntilBucketsSynced(tableBuckets);
    }

    private Set<Integer> totalBucketsOfPartition(TablePath tablePath, String partition)
            throws Exception {
        FileStoreTable fileStoreTable =
                (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        List<Split> splits =
                fileStoreTable
                        .newReadBuilder()
                        .withPartitionFilter(Collections.singletonMap("c", partition))
                        .newScan()
                        .plan()
                        .splits();
        assertThat(splits).isNotEmpty();
        Set<Integer> totalBuckets = new HashSet<>();
        for (Split split : splits) {
            totalBuckets.add(((DataSplit) split).totalBuckets());
        }
        return totalBuckets;
    }
}
