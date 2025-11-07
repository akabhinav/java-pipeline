package com.enterprise.pipeline.quality.profiler;

import java.io.Serializable;
import java.util.Map;

/**
 * Profile information for a single column.
 * Contains statistics, null analysis, type information, and value distributions.
 */
public class ColumnProfile implements Serializable {
    private final String columnName;
    private final String dataType;
    private final long totalCount;
    private final long nullCount;
    private final long distinctCount;

    // Numeric statistics (null if not numeric)
    private final Double min;
    private final Double max;
    private final Double mean;
    private final Double median;
    private final Double stdDev;

    // String statistics (null if not string)
    private final Integer minLength;
    private final Integer maxLength;
    private final Double avgLength;

    // Value distribution (top N values with counts)
    private final Map<String, Long> valueDistribution;

    // Data quality indicators
    private final double completenessPercent;
    private final double uniquenessPercent;

    private ColumnProfile(Builder builder) {
        this.columnName = builder.columnName;
        this.dataType = builder.dataType;
        this.totalCount = builder.totalCount;
        this.nullCount = builder.nullCount;
        this.distinctCount = builder.distinctCount;
        this.min = builder.min;
        this.max = builder.max;
        this.mean = builder.mean;
        this.median = builder.median;
        this.stdDev = builder.stdDev;
        this.minLength = builder.minLength;
        this.maxLength = builder.maxLength;
        this.avgLength = builder.avgLength;
        this.valueDistribution = builder.valueDistribution;

        // Calculate quality indicators
        this.completenessPercent = totalCount > 0 ?
            ((totalCount - nullCount) * 100.0 / totalCount) : 0.0;
        this.uniquenessPercent = totalCount > 0 ?
            (distinctCount * 100.0 / totalCount) : 0.0;
    }

    public String getColumnName() { return columnName; }
    public String getDataType() { return dataType; }
    public long getTotalCount() { return totalCount; }
    public long getNullCount() { return nullCount; }
    public long getDistinctCount() { return distinctCount; }
    public Double getMin() { return min; }
    public Double getMax() { return max; }
    public Double getMean() { return mean; }
    public Double getMedian() { return median; }
    public Double getStdDev() { return stdDev; }
    public Integer getMinLength() { return minLength; }
    public Integer getMaxLength() { return maxLength; }
    public Double getAvgLength() { return avgLength; }
    public Map<String, Long> getValueDistribution() { return valueDistribution; }
    public double getCompletenessPercent() { return completenessPercent; }
    public double getUniquenessPercent() { return uniquenessPercent; }

    public boolean isNumeric() {
        return dataType.contains("Int") || dataType.contains("Long") ||
               dataType.contains("Double") || dataType.contains("Float") ||
               dataType.contains("Decimal");
    }

    public boolean isString() {
        return dataType.contains("String");
    }

    public static Builder builder(String columnName) {
        return new Builder(columnName);
    }

    public static class Builder {
        private final String columnName;
        private String dataType;
        private long totalCount;
        private long nullCount;
        private long distinctCount;
        private Double min;
        private Double max;
        private Double mean;
        private Double median;
        private Double stdDev;
        private Integer minLength;
        private Integer maxLength;
        private Double avgLength;
        private Map<String, Long> valueDistribution;

        public Builder(String columnName) {
            this.columnName = columnName;
        }

        public Builder dataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder totalCount(long totalCount) {
            this.totalCount = totalCount;
            return this;
        }

        public Builder nullCount(long nullCount) {
            this.nullCount = nullCount;
            return this;
        }

        public Builder distinctCount(long distinctCount) {
            this.distinctCount = distinctCount;
            return this;
        }

        public Builder min(Double min) {
            this.min = min;
            return this;
        }

        public Builder max(Double max) {
            this.max = max;
            return this;
        }

        public Builder mean(Double mean) {
            this.mean = mean;
            return this;
        }

        public Builder median(Double median) {
            this.median = median;
            return this;
        }

        public Builder stdDev(Double stdDev) {
            this.stdDev = stdDev;
            return this;
        }

        public Builder minLength(Integer minLength) {
            this.minLength = minLength;
            return this;
        }

        public Builder maxLength(Integer maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public Builder avgLength(Double avgLength) {
            this.avgLength = avgLength;
            return this;
        }

        public Builder valueDistribution(Map<String, Long> valueDistribution) {
            this.valueDistribution = valueDistribution;
            return this;
        }

        public ColumnProfile build() {
            return new ColumnProfile(this);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ColumnProfile{")
          .append("name='").append(columnName).append('\'')
          .append(", type='").append(dataType).append('\'')
          .append(", total=").append(totalCount)
          .append(", nulls=").append(nullCount)
          .append(", distinct=").append(distinctCount)
          .append(", completeness=").append(String.format("%.2f%%", completenessPercent))
          .append(", uniqueness=").append(String.format("%.2f%%", uniquenessPercent));

        if (isNumeric()) {
            sb.append(", min=").append(min)
              .append(", max=").append(max)
              .append(", mean=").append(String.format("%.2f", mean))
              .append(", stdDev=").append(String.format("%.2f", stdDev));
        }

        if (isString()) {
            sb.append(", minLen=").append(minLength)
              .append(", maxLen=").append(maxLength)
              .append(", avgLen=").append(String.format("%.2f", avgLength));
        }

        sb.append('}');
        return sb.toString();
    }
}
