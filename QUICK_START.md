# Quick Start Guide - Windows

## 30-Second Start

```cmd
# 1. Start everything
start-local-environment.bat

# 2. Wait 2 minutes, then submit job
run-pipeline-on-cluster.bat RealConnectorsPipeline

# 3. View results
start http://localhost:8081
```

---

## Common Commands

### Start Services
```cmd
start-local-environment.bat
```

### Submit Job
```cmd
run-pipeline-on-cluster.bat RealConnectorsPipeline
```

### Rebuild Project
```cmd
mvn clean package -DskipTests
```

### Check Status
```cmd
docker ps
start http://localhost:8081
```

### Stop Everything
```cmd
docker-compose down
```

---

## Service URLs

| Service | URL |
|---------|-----|
| Spark Master UI | http://localhost:8081 |
| Spark Worker 1 | http://localhost:8082 |
| Spark Worker 2 | http://localhost:8083 |
| MinIO Console | http://localhost:9003 |
| Kafka UI | http://localhost:8084 |
| pgAdmin | http://localhost:5050 |

**Credentials:**
- MinIO: `minioadmin` / `minioadmin`
- pgAdmin: `admin@admin.com` / `admin`
- PostgreSQL: `pipeline` / `pipeline123`

---

## Troubleshooting Quick Fixes

### Job not in Spark UI?
Check `RealConnectorsPipeline.java` line 55-66.
Remove any `.master("local[*]")` line.

### Port conflicts?
Ports are already adjusted:
- PostgreSQL: 5434 (not 5432)
- MinIO: 9002/9003 (not 9000/9001)
- Kafka UI: 8084 (not 8080)

### Connection refused?
Use Docker service names in code:
- `postgres:5432` (not localhost:5432)
- `minio:9000` (not localhost:9000)
- `kafka:29092` (not localhost:9092)

### Build errors?
```cmd
mvn clean package -DskipTests
```

---

## Verify Success

1. **Check Spark UI:** http://localhost:8081
   - Should show: "1 Completed" application
   - Executors: 4 (2 per worker)

2. **Check PostgreSQL:**
   ```cmd
   docker exec -it pipeline-postgres psql -U pipeline -d banking -c "SELECT COUNT(*) FROM loan_approval_results;"
   ```

3. **Check MinIO:** http://localhost:9003
   - Bucket: `pipeline-output`
   - Folder: `loan-approval-results/`

---

**Full Documentation:** See `WINDOWS_DEPLOYMENT_GUIDE.md`
