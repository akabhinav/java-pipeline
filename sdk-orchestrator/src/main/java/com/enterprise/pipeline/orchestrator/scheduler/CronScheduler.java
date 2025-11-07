package com.enterprise.pipeline.orchestrator.scheduler;

import com.enterprise.pipeline.orchestrator.dag.DAG;
import com.enterprise.pipeline.orchestrator.executor.DAGExecutor;
import com.enterprise.pipeline.orchestrator.executor.DAGExecutionResult;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cron-based Scheduler
 *
 * Schedules DAG execution based on cron expressions
 *
 * Examples:
 * - "0 0 * * *"     - Daily at midnight
 * - "0 */6 * * *"   - Every 6 hours
 * - "0 0 * * MON"   - Every Monday at midnight
 * - "0 0 1 * *"     - First day of month at midnight
 *
 * @author Enterprise Pipeline Platform
 */
public class CronScheduler {

    private static final Logger logger = LoggerFactory.getLogger(CronScheduler.class);

    private final Scheduler quartzScheduler;
    private final DAGExecutor dagExecutor;
    private final Map<String, ScheduledDAG> scheduledDAGs;

    public CronScheduler(DAGExecutor dagExecutor) throws SchedulerException {
        this.dagExecutor = dagExecutor;
        this.quartzScheduler = StdSchedulerFactory.getDefaultScheduler();
        this.scheduledDAGs = new ConcurrentHashMap<>();
    }

    /**
     * Schedule DAG execution
     */
    public void schedule(DAG dag, String cronExpression, Map<String, Object> parameters) throws SchedulerException {
        String dagId = dag.getName();

        logger.info("Scheduling DAG {} with cron: {}", dagId, cronExpression);

        // Create job
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("dag", dag);
        jobDataMap.put("dagExecutor", dagExecutor);
        jobDataMap.put("parameters", parameters);

        JobDetail job = JobBuilder.newJob(DAGExecutionJob.class)
            .withIdentity(dagId, "dags")
            .setJobData(jobDataMap)
            .build();

        // Create trigger
        CronTrigger trigger = TriggerBuilder.newTrigger()
            .withIdentity(dagId + "-trigger", "dags")
            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
            .build();

        // Schedule
        quartzScheduler.scheduleJob(job, trigger);

        scheduledDAGs.put(dagId, new ScheduledDAG(dag, cronExpression, parameters));

        logger.info("DAG {} scheduled successfully", dagId);
    }

    /**
     * Start scheduler
     */
    public void start() throws SchedulerException {
        logger.info("Starting scheduler");
        quartzScheduler.start();
        logger.info("Scheduler started");
    }

    /**
     * Stop scheduler
     */
    public void stop() throws SchedulerException {
        logger.info("Stopping scheduler");
        quartzScheduler.shutdown(true);
        logger.info("Scheduler stopped");
    }

    /**
     * Unschedule DAG
     */
    public void unschedule(String dagId) throws SchedulerException {
        logger.info("Unscheduling DAG {}", dagId);
        quartzScheduler.deleteJob(new JobKey(dagId, "dags"));
        scheduledDAGs.remove(dagId);
        logger.info("DAG {} unscheduled", dagId);
    }

    /**
     * Get all scheduled DAGs
     */
    public Map<String, ScheduledDAG> getScheduledDAGs() {
        return Map.copyOf(scheduledDAGs);
    }

    /**
     * Quartz Job that executes DAG
     */
    public static class DAGExecutionJob implements Job {
        private static final Logger logger = LoggerFactory.getLogger(DAGExecutionJob.class);

        @Override
        public void execute(JobExecutionContext context) {
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();

            DAG dag = (DAG) dataMap.get("dag");
            DAGExecutor executor = (DAGExecutor) dataMap.get("dagExecutor");
            @SuppressWarnings("unchecked")
            Map<String, Object> parameters = (Map<String, Object>) dataMap.get("parameters");

            logger.info("Executing scheduled DAG: {}", dag.getName());

            try {
                DAGExecutionResult result = executor.execute(dag, parameters);
                logger.info("Scheduled DAG {} completed: {}", dag.getName(), result.getOverallStatus());
            } catch (Exception e) {
                logger.error("Scheduled DAG {} failed", dag.getName(), e);
            }
        }
    }

    /**
     * Scheduled DAG info
     */
    public static class ScheduledDAG {
        private final DAG dag;
        private final String cronExpression;
        private final Map<String, Object> parameters;

        public ScheduledDAG(DAG dag, String cronExpression, Map<String, Object> parameters) {
            this.dag = dag;
            this.cronExpression = cronExpression;
            this.parameters = Map.copyOf(parameters);
        }

        public DAG getDag() { return dag; }
        public String getCronExpression() { return cronExpression; }
        public Map<String, Object> getParameters() { return parameters; }

        @Override
        public String toString() {
            return String.format("ScheduledDAG{dag=%s, cron=%s}",
                dag.getName(), cronExpression);
        }
    }
}
