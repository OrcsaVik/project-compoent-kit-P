package com.github.framework.database.hashalgo;

import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.sharding.algorithm.sharding.ShardingAutoTableAlgorithmUtil;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.ShardingAlgorithmInitializationException;

import java.util.Collection;
import java.util.Properties;

/**
 * @author Vik
 * @date 2025-09-04
 * @description
 */
public final class CustomDbHashModShardingAlgorithm implements StandardShardingAlgorithm<Comparable<?>> {

    private static final String SHARDING_COUNT_KEY = "sharding-count";
    private static final String TABLE_SHARDING_COUNT_KEY = "table-sharding-count";

    private int shardingCount;
    private int tableShardingCount;

    @Override
    public void init(final Properties props) {
        shardingCount = getShardingCount(props);
        tableShardingCount = getTableShardingCount(props);
    }

    /**
     * 哈希分片算法的核心实现
     * 公式：shardingValue.getValue()) % shardingCount / tableShardingCount
     *
     * @param availableTargetNames 可用的目标表名集合（例如：t_order_0, t_order_1 等）
     * @param shardingValue 分片键值对象，包含了用于分片的具体值
     * @return 匹配的目标表名
     */
    @Override
    public String doSharding(final Collection<String> availableTargetNames,
                             final PreciseShardingValue<Comparable<?>> shardingValue) {
        // 1. 计算哈希分片值并生成表后缀
        // hashShardingValue：对分片键值进行哈希计算（具体实现未展示）
        // % shardingCount：对总分片数取模，确定落在哪个分片区间
        // / tableShardingCount：除以每个数据库下的表数量，得到表的索引
        String suffix = String.valueOf(hashShardingValue(shardingValue.getValue())
                % shardingCount
                / tableShardingCount);

        // 2. 根据计算出的后缀查找匹配的目标表名
        // ShardingAutoTableAlgorithmUtil：分片工具类
        // findMatchedTargetName：从可用表名中找到与后缀匹配的表
        // orElse(null)：如果没有找到匹配的表则返回null
        return ShardingAutoTableAlgorithmUtil.findMatchedTargetName(
                availableTargetNames,
                suffix,
                shardingValue.getDataNodeInfo()
        ).orElse(null);
    }

    /**
     * 范围分片处理（本实现中未做特殊处理，返回所有可用表）
     *
     * @param availableTargetNames 可用的目标表名集合
     * @param shardingValue 范围分片键值对象（包含起始值和结束值）
     * @return 所有可用的表名集合
     */
    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames,
                                         final RangeShardingValue<Comparable<?>> shardingValue) {
        // 这里直接返回所有可用表名，意味着当使用范围查询时（如 between and）
        // 会扫描所有表，可能影响性能。实际应用中应根据业务需求实现具体的范围分片逻辑
        return availableTargetNames;
    }

    private int getShardingCount(final Properties props) {
        ShardingSpherePreconditions.checkState(props.containsKey(SHARDING_COUNT_KEY), () -> new ShardingAlgorithmInitializationException(getType(), "Sharding count cannot be null."));
        return Integer.parseInt(props.getProperty(SHARDING_COUNT_KEY));
    }

    private int getTableShardingCount(final Properties props) {
        ShardingSpherePreconditions.checkState(props.containsKey(TABLE_SHARDING_COUNT_KEY), () -> new ShardingAlgorithmInitializationException(getType(), "Table sharding count cannot be null."));
        return Integer.parseInt(props.getProperty(TABLE_SHARDING_COUNT_KEY));
    }

    private long hashShardingValue(final Object shardingValue) {
        return Math.abs((long) shardingValue.hashCode());
    }

    @Override
    public String getType() {
        return "CLASS_BASED";
    }
}