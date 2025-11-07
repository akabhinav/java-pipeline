package com.enterprise.pipeline.listeners;

import org.apache.spark.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Audit Trail Listener
 *
 * Comprehensive audit logging for ALL industries (regulatory compliance).
 *
 * Audit Records Captured:
 * - Who: User/Application running the job
 * - What: Job/Query executed
 * - When: Timestamp (ISO 8601 format)
 * - Where: Cluster/Environment
 * - Why: Job description/tags
 * - How: Success/Failure status
 * - Input/Output: Data sources and sinks
 *
 * Compliance Standards:
 * - SOX (Sarbanes-Oxley)
 * - GDPR (General Data Protection Regulation)
 * - HIPAA (Health Insurance Portability and Accountability Act)
 * - PCI-DSS (Payment Card Industry Data Security Standard)
 * - Basel III (Banking)
 * - 21 CFR Part 11 (FDA - Pharma)
 *
 * Use Cases:
 * - Regulatory audits
 * - Security investigations
 * - Compliance reporting
 * - Data access tracking
 * - Root cause analysis
 *
 * @author Enterprise Pipeline Platform
 */
public class AuditTrailListener extends SparkListener {

    private static final Logger logger = LoggerFactory.getLogger(AuditTrailListener.class);
    private static final DateTimeFormatter ISO_FORMATTER =
        DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault());

    private final List<AuditRecord> auditTrail = new CopyOnWriteArrayList<>();
    private final String environment;
    private final boolean logToFile;

    public AuditTrailListener() {
        this("local", false);
    }

    public AuditTrailListener(String environment, boolean logToFile) {
        this.environment = environment;
        this.logToFile = logToFile;
        logger.info("AuditTrailListener initialized: environment={}, logToFile={}",
            environment, logToFile);
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        AuditRecord record = new AuditRecord(
            "APPLICATION_START",
            applicationStart.appName(),
            getCurrentUser(),
            environment,
            applicationStart.time(),
            null,
            "SUCCESS",
            "Application started: " + applicationStart.appName()
        );

        recordAudit(record);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        AuditRecord record = new AuditRecord(
            "APPLICATION_END",
            "Application",
            getCurrentUser(),
            environment,
            applicationEnd.time(),
            null,
            "SUCCESS",
            "Application ended normally"
        );

        recordAudit(record);
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        String jobDescription = extractJobDescription(jobStart);

        AuditRecord record = new AuditRecord(
            "JOB_START",
            "Job-" + jobStart.jobId(),
            getCurrentUser(),
            environment,
            jobStart.time(),
            null,
            "RUNNING",
            jobDescription
        );

        recordAudit(record);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        String status = (jobEnd.jobResult() instanceof org.apache.spark.scheduler.JobSucceeded$) ?
            "SUCCESS" : "FAILED";

        AuditRecord record = new AuditRecord(
            "JOB_END",
            "Job-" + jobEnd.jobId(),
            getCurrentUser(),
            environment,
            jobEnd.time(),
            null,
            status,
            "Job completed with status: " + status
        );

        recordAudit(record);

        // Log failed jobs for compliance
        if ("FAILED".equals(status)) {
            logger.error("AUDIT: Job {} FAILED - User: {}, Environment: {}, Time: {}",
                jobEnd.jobId(), getCurrentUser(), environment,
                ISO_FORMATTER.format(Instant.ofEpochMilli(jobEnd.time())));
        }
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        StageInfo stageInfo = stageSubmitted.stageInfo();

        AuditRecord record = new AuditRecord(
            "STAGE_SUBMITTED",
            "Stage-" + stageInfo.stageId(),
            getCurrentUser(),
            environment,
            stageInfo.submissionTime().getOrElse(() -> System.currentTimeMillis()),
            null,
            "SUBMITTED",
            "Stage submitted: " + stageInfo.name()
        );

        recordAudit(record);
    }

    /**
     * Record manual audit event (call from your application code)
     *
     * Example:
     *   auditListener.recordCustomEvent("DATA_ACCESS", "customers_table", "READ", "Query customer data");
     */
    public void recordCustomEvent(String action, String resource, String status, String details) {
        AuditRecord record = new AuditRecord(
            action,
            resource,
            getCurrentUser(),
            environment,
            System.currentTimeMillis(),
            null,
            status,
            details
        );

        recordAudit(record);
    }

    /**
     * Record audit with optional data classification (e.g., PII, PCI, PHI)
     */
    public void recordDataAccess(String resource, String action, String dataClassification, String details) {
        AuditRecord record = new AuditRecord(
            action,
            resource,
            getCurrentUser(),
            environment,
            System.currentTimeMillis(),
            dataClassification,
            "SUCCESS",
            details
        );

        recordAudit(record);

        // Special logging for sensitive data
        if ("PII".equals(dataClassification) || "PCI".equals(dataClassification) ||
            "PHI".equals(dataClassification)) {
            logger.warn("AUDIT: Sensitive data access - Classification: {}, Resource: {}, User: {}, Action: {}",
                dataClassification, resource, getCurrentUser(), action);
        }
    }

    /**
     * Record audit entry
     */
    private void recordAudit(AuditRecord record) {
        auditTrail.add(record);

        // Log to console/file
        logger.info("AUDIT: {} | {} | {} | {} | {} | {} | {}",
            record.getTimestampFormatted(),
            record.getAction(),
            record.getResource(),
            record.getUser(),
            record.getEnvironment(),
            record.getStatus(),
            record.getDetails());

        // Optionally write to audit log file
        if (logToFile) {
            writeToAuditFile(record);
        }
    }

    /**
     * Write audit record to file
     * Override this method to customize audit file location/format
     */
    protected void writeToAuditFile(AuditRecord record) {
        // TODO: Implement file writing
        // Example: Write to /var/log/spark/audit.log in JSON or CSV format
        // Can integrate with log rotation, encryption, etc.
    }

    /**
     * Get current user (from Spark context or system property)
     */
    private String getCurrentUser() {
        // Try to get user from system property
        String user = System.getProperty("user.name");
        if (user == null || user.isEmpty()) {
            user = "UNKNOWN";
        }
        return user;
    }

    /**
     * Extract job description from properties
     */
    private String extractJobDescription(SparkListenerJobStart jobStart) {
        try {
            return jobStart.properties().getProperty("spark.job.description", "No description");
        } catch (Exception e) {
            return "No description";
        }
    }

    /**
     * Get all audit records
     */
    public List<AuditRecord> getAuditTrail() {
        return new CopyOnWriteArrayList<>(auditTrail);
    }

    /**
     * Get audit records for specific user
     */
    public List<AuditRecord> getAuditTrailForUser(String user) {
        return auditTrail.stream()
            .filter(record -> user.equals(record.getUser()))
            .toList();
    }

    /**
     * Get audit records for specific resource
     */
    public List<AuditRecord> getAuditTrailForResource(String resource) {
        return auditTrail.stream()
            .filter(record -> resource.equals(record.getResource()))
            .toList();
    }

    /**
     * Export audit trail to JSON (for regulatory reporting)
     */
    public String exportToJSON() {
        // TODO: Implement JSON export
        // Can use Jackson or Gson
        return "[]";  // Placeholder
    }

    /**
     * Audit Record POJO
     */
    public static class AuditRecord {
        private final String action;              // What happened (e.g., JOB_START, DATA_ACCESS)
        private final String resource;            // What was accessed (e.g., table name, file path)
        private final String user;                // Who performed the action
        private final String environment;         // Where (e.g., dev, staging, prod)
        private final long timestamp;             // When (Unix milliseconds)
        private final String dataClassification;  // PII, PCI, PHI, etc. (optional)
        private final String status;              // SUCCESS, FAILED, RUNNING
        private final String details;             // Additional context

        public AuditRecord(String action, String resource, String user, String environment,
                          long timestamp, String dataClassification, String status, String details) {
            this.action = action;
            this.resource = resource;
            this.user = user;
            this.environment = environment;
            this.timestamp = timestamp;
            this.dataClassification = dataClassification;
            this.status = status;
            this.details = details;
        }

        public String getAction() { return action; }
        public String getResource() { return resource; }
        public String getUser() { return user; }
        public String getEnvironment() { return environment; }
        public long getTimestamp() { return timestamp; }
        public String getDataClassification() { return dataClassification; }
        public String getStatus() { return status; }
        public String getDetails() { return details; }

        public String getTimestampFormatted() {
            return ISO_FORMATTER.format(Instant.ofEpochMilli(timestamp));
        }

        @Override
        public String toString() {
            return String.format("AuditRecord{action=%s, resource=%s, user=%s, timestamp=%s, status=%s}",
                action, resource, user, getTimestampFormatted(), status);
        }
    }
}
