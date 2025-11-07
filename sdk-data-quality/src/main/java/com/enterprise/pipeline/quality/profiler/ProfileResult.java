package com.enterprise.pipeline.quality.profiler;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Complete profiling result for a dataset.
 * Contains profiles for all columns and dataset-level statistics.
 */
public class ProfileResult implements Serializable {
    private final String datasetName;
    private final Instant profiledAt;
    private final long totalRows;
    private final int totalColumns;
    private final List<ColumnProfile> columnProfiles;

    // Dataset-level quality metrics
    private final double overallCompletenessPercent;
    private final int columnsWithNulls;
    private final int columnsWithDuplicates;

    public ProfileResult(String datasetName,
                        long totalRows,
                        List<ColumnProfile> columnProfiles) {
        this.datasetName = datasetName;
        this.profiledAt = Instant.now();
        this.totalRows = totalRows;
        this.totalColumns = columnProfiles.size();
        this.columnProfiles = columnProfiles;

        // Calculate dataset-level metrics
        this.overallCompletenessPercent = columnProfiles.stream()
            .mapToDouble(ColumnProfile::getCompletenessPercent)
            .average()
            .orElse(0.0);

        this.columnsWithNulls = (int) columnProfiles.stream()
            .filter(cp -> cp.getNullCount() > 0)
            .count();

        this.columnsWithDuplicates = (int) columnProfiles.stream()
            .filter(cp -> cp.getDistinctCount() < cp.getTotalCount())
            .count();
    }

    public String getDatasetName() { return datasetName; }
    public Instant getProfiledAt() { return profiledAt; }
    public long getTotalRows() { return totalRows; }
    public int getTotalColumns() { return totalColumns; }
    public List<ColumnProfile> getColumnProfiles() { return columnProfiles; }
    public double getOverallCompletenessPercent() { return overallCompletenessPercent; }
    public int getColumnsWithNulls() { return columnsWithNulls; }
    public int getColumnsWithDuplicates() { return columnsWithDuplicates; }

    public ColumnProfile getColumnProfile(String columnName) {
        return columnProfiles.stream()
            .filter(cp -> cp.getColumnName().equals(columnName))
            .findFirst()
            .orElse(null);
    }

    public Map<String, ColumnProfile> getColumnProfilesMap() {
        return columnProfiles.stream()
            .collect(Collectors.toMap(
                ColumnProfile::getColumnName,
                cp -> cp
            ));
    }

    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("=".repeat(80)).append("\n");
        sb.append("DATA QUALITY PROFILE REPORT\n");
        sb.append("=".repeat(80)).append("\n");
        sb.append("Dataset: ").append(datasetName).append("\n");
        sb.append("Profiled at: ").append(profiledAt).append("\n");
        sb.append("Total Rows: ").append(totalRows).append("\n");
        sb.append("Total Columns: ").append(totalColumns).append("\n");
        sb.append("Overall Completeness: ").append(String.format("%.2f%%", overallCompletenessPercent)).append("\n");
        sb.append("Columns with Nulls: ").append(columnsWithNulls).append("\n");
        sb.append("Columns with Duplicates: ").append(columnsWithDuplicates).append("\n");
        sb.append("=".repeat(80)).append("\n\n");

        sb.append("COLUMN PROFILES:\n");
        sb.append("-".repeat(80)).append("\n");
        for (ColumnProfile cp : columnProfiles) {
            sb.append(cp.toString()).append("\n");
        }
        sb.append("=".repeat(80)).append("\n");

        return sb.toString();
    }

    @Override
    public String toString() {
        return getSummary();
    }
}
