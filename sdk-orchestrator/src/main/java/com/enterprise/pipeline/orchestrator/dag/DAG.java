package com.enterprise.pipeline.orchestrator.dag;

import com.enterprise.pipeline.orchestrator.core.Job;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Directed Acyclic Graph (DAG) for Workflow Management
 *
 * Represents dependencies between jobs:
 * - Job A → Job B means "Job B depends on Job A"
 * - Job B runs only after Job A completes successfully
 *
 * Features:
 * - Cycle detection
 * - Topological sorting
 * - Parallel execution planning
 *
 * @author Enterprise Pipeline Platform
 */
public class DAG {

    private final String name;
    private final Map<String, Job> jobs;
    private final Map<String, List<String>> dependencies;  // job -> list of jobs it depends on
    private final Map<String, List<String>> dependents;    // job -> list of jobs that depend on it

    private DAG(String name, Map<String, Job> jobs, Map<String, List<String>> dependencies) {
        this.name = name;
        this.jobs = new ConcurrentHashMap<>(jobs);
        this.dependencies = new ConcurrentHashMap<>(dependencies);
        this.dependents = new ConcurrentHashMap<>();

        // Build reverse dependency map
        buildDependentsMap();

        // Validate DAG (no cycles)
        validate();
    }

    /**
     * Build reverse dependency map (dependents)
     */
    private void buildDependentsMap() {
        for (Map.Entry<String, List<String>> entry : dependencies.entrySet()) {
            String job = entry.getKey();
            List<String> deps = entry.getValue();

            for (String dep : deps) {
                dependents.computeIfAbsent(dep, k -> new ArrayList<>()).add(job);
            }
        }
    }

    /**
     * Validate DAG - check for cycles
     */
    private void validate() {
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();

        for (String jobId : jobs.keySet()) {
            if (hasCycle(jobId, visited, recursionStack)) {
                throw new IllegalStateException("DAG contains a cycle involving job: " + jobId);
            }
        }
    }

    /**
     * Detect cycle using DFS
     */
    private boolean hasCycle(String jobId, Set<String> visited, Set<String> recursionStack) {
        if (recursionStack.contains(jobId)) {
            return true;  // Cycle detected
        }

        if (visited.contains(jobId)) {
            return false;  // Already processed
        }

        visited.add(jobId);
        recursionStack.add(jobId);

        // Check all dependencies
        List<String> deps = dependencies.getOrDefault(jobId, List.of());
        for (String dep : deps) {
            if (hasCycle(dep, visited, recursionStack)) {
                return true;
            }
        }

        recursionStack.remove(jobId);
        return false;
    }

    /**
     * Get topological sort of jobs (execution order)
     */
    public List<String> getTopologicalOrder() {
        Map<String, Integer> inDegree = new HashMap<>();
        Queue<String> queue = new LinkedList<>();
        List<String> result = new ArrayList<>();

        // Calculate in-degree for each job
        for (String job : jobs.keySet()) {
            inDegree.put(job, dependencies.getOrDefault(job, List.of()).size());
            if (inDegree.get(job) == 0) {
                queue.offer(job);
            }
        }

        // Kahn's algorithm
        while (!queue.isEmpty()) {
            String job = queue.poll();
            result.add(job);

            // Reduce in-degree for dependents
            for (String dependent : dependents.getOrDefault(job, List.of())) {
                inDegree.put(dependent, inDegree.get(dependent) - 1);
                if (inDegree.get(dependent) == 0) {
                    queue.offer(dependent);
                }
            }
        }

        if (result.size() != jobs.size()) {
            throw new IllegalStateException("DAG contains a cycle");
        }

        return result;
    }

    /**
     * Get jobs that have no dependencies (can run first)
     */
    public List<String> getRootJobs() {
        return jobs.keySet().stream()
            .filter(jobId -> dependencies.getOrDefault(jobId, List.of()).isEmpty())
            .collect(Collectors.toList());
    }

    /**
     * Get jobs that no other job depends on (last jobs)
     */
    public List<String> getLeafJobs() {
        return jobs.keySet().stream()
            .filter(jobId -> dependents.getOrDefault(jobId, List.of()).isEmpty())
            .collect(Collectors.toList());
    }

    /**
     * Get jobs that can run in parallel at each level
     */
    public List<Set<String>> getExecutionLevels() {
        List<Set<String>> levels = new ArrayList<>();
        Map<String, Integer> inDegree = new HashMap<>();
        Set<String> completed = new HashSet<>();

        // Initialize in-degree
        for (String job : jobs.keySet()) {
            inDegree.put(job, dependencies.getOrDefault(job, List.of()).size());
        }

        while (completed.size() < jobs.size()) {
            // Find jobs that can run now (in-degree = 0 and not completed)
            Set<String> currentLevel = jobs.keySet().stream()
                .filter(job -> !completed.contains(job) && inDegree.get(job) == 0)
                .collect(Collectors.toSet());

            if (currentLevel.isEmpty()) {
                throw new IllegalStateException("No jobs can run - possible cycle");
            }

            levels.add(currentLevel);
            completed.addAll(currentLevel);

            // Update in-degree for dependents
            for (String job : currentLevel) {
                for (String dependent : dependents.getOrDefault(job, List.of())) {
                    inDegree.put(dependent, inDegree.get(dependent) - 1);
                }
            }
        }

        return levels;
    }

    // Getters
    public String getName() { return name; }
    public Job getJob(String jobId) { return jobs.get(jobId); }
    public Map<String, Job> getAllJobs() { return Map.copyOf(jobs); }
    public List<String> getDependencies(String jobId) {
        return List.copyOf(dependencies.getOrDefault(jobId, List.of()));
    }
    public List<String> getDependents(String jobId) {
        return List.copyOf(dependents.getOrDefault(jobId, List.of()));
    }
    public int getJobCount() { return jobs.size(); }

    /**
     * DAG Builder
     */
    public static class Builder {
        private String name;
        private final Map<String, Job> jobs = new HashMap<>();
        private final Map<String, List<String>> dependencies = new HashMap<>();

        public Builder(String name) {
            this.name = name;
        }

        public Builder addJob(Job job) {
            jobs.put(job.getId(), job);
            return this;
        }

        public Builder addDependency(String jobId, String dependsOn) {
            if (!jobs.containsKey(jobId)) {
                throw new IllegalArgumentException("Job not found: " + jobId);
            }
            if (!jobs.containsKey(dependsOn)) {
                throw new IllegalArgumentException("Dependency job not found: " + dependsOn);
            }

            dependencies.computeIfAbsent(jobId, k -> new ArrayList<>()).add(dependsOn);
            return this;
        }

        public Builder addDependencies(String jobId, List<String> dependsOn) {
            for (String dep : dependsOn) {
                addDependency(jobId, dep);
            }
            return this;
        }

        public DAG build() {
            return new DAG(name, jobs, dependencies);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DAG{name=").append(name).append(", jobs=").append(jobs.size()).append("}\n");

        List<String> order = getTopologicalOrder();
        for (String jobId : order) {
            List<String> deps = dependencies.getOrDefault(jobId, List.of());
            sb.append("  ").append(jobId);
            if (!deps.isEmpty()) {
                sb.append(" <- ").append(deps);
            }
            sb.append("\n");
        }

        return sb.toString();
    }
}
