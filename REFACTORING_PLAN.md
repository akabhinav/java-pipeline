# Refactoring Plan - Modular Architecture

## Current Structure
```
data-pipeline-platform/
├── pipeline-sdk/
│   ├── sdk-api/
│   ├── sdk-core/
│   └── sdk-transformations/  (contains both transformations AND connectors)
└── pipeline-examples/
```

## Target Structure
```
data-pipeline-platform/
├── pipeline-sdk/              (LAYER 1: Core SDK - Standalone)
│   ├── sdk-api/              ✅ Already exists
│   ├── sdk-core/             ✅ Already exists
│   ├── sdk-transformations/  ✅ Already exists (will split)
│   ├── sdk-connectors/       🆕 NEW - Extract from sdk-transformations
│   └── sdk-client/           🆕 NEW - Java/Python/REST clients
│
├── pipeline-services/         (LAYER 2: Platform Services)
│   ├── orchestrator-service/     🆕 NEW - Workflow orchestration
│   ├── catalog-service/          🆕 NEW - Metadata & lineage
│   ├── quality-service/          🆕 NEW - Data quality
│   ├── governance-service/       🆕 NEW - Security & compliance
│   ├── monitoring-service/       🆕 NEW - Monitoring & alerts
│   ├── optimizer-service/        🆕 NEW - Query optimization
│   ├── ml-service/               🆕 NEW - ML integration
│   └── api-gateway/              🆕 NEW - API Gateway
│
├── pipeline-ui/               (LAYER 3: User Interface)
│   ├── ui-designer/              🆕 NEW - Visual designer (React)
│   └── ui-admin/                 🆕 NEW - Admin console (React)
│
├── pipeline-tools/            (LAYER 4: Integration & Tools)
│   ├── cli/                      🆕 NEW - Command-line interface
│   ├── terraform-provider/       🆕 NEW - Terraform provider
│   └── sdk-python/               🆕 NEW - Python SDK
│
└── pipeline-examples/         ✅ Already exists
```

---

## Phase-by-Phase Refactoring

### Phase 1: Extract Connectors from Transformations ✅ NOW

**Goal**: Separate connectors into their own module for independent usage.

**Steps**:
1. Create `sdk-connectors` module
2. Move connector classes from `sdk-transformations` to `sdk-connectors`
3. Update META-INF/services registrations
4. Update dependencies

**Files to Move**:
```
sdk-transformations/src/main/java/com/enterprise/pipeline/connector/
├── FileSource.java       → sdk-connectors/
├── FileSink.java         → sdk-connectors/
├── JdbcSource.java       → sdk-connectors/
├── JdbcSink.java         → sdk-connectors/
├── S3Source.java         → sdk-connectors/
├── S3Sink.java           → sdk-connectors/
├── KafkaSource.java      → sdk-connectors/
└── KafkaSink.java        → sdk-connectors/

sdk-transformations/src/main/resources/META-INF/services/
├── com.enterprise.pipeline.api.Source  → sdk-connectors/
└── com.enterprise.pipeline.api.Sink    → sdk-connectors/
```

**Benefit**: Users can use connectors independently without transformation dependencies.

---

### Phase 2: Create SDK Client Library

**Goal**: Provide programmatic access to SDK from Java/Python/REST.

**New Module**: `sdk-client`

**Components**:
```java
// Java Client
public class PipelineClient {
    public PipelineExecutionResult execute(PipelineConfig config);
    public PipelineValidationResult validate(PipelineConfig config);
    public List<Transformation> listTransformations();
    public TransformationMetadata getTransformation(String name);
}

// REST Client
public class RestPipelineClient {
    public PipelineExecutionResult executeRemote(String apiUrl, PipelineConfig config);
}
```

---

### Phase 3: Create Orchestrator Service

**Goal**: Workflow orchestration, scheduling, and execution management.

**New Module**: `orchestrator-service` (Spring Boot)

**Key Features**:
- Cron scheduling
- DAG workflow execution
- Dependency management
- Retry logic
- State persistence

**Database Schema**:
```sql
CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    definition JSONB NOT NULL,
    schedule VARCHAR(100),
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY,
    workflow_id UUID REFERENCES workflows(id),
    status VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    error_message TEXT,
    metrics JSONB
);

CREATE TABLE workflow_dependencies (
    workflow_id UUID REFERENCES workflows(id),
    depends_on_workflow_id UUID REFERENCES workflows(id),
    PRIMARY KEY (workflow_id, depends_on_workflow_id)
);
```

**REST API**:
```
POST   /api/v1/workflows                 - Create workflow
GET    /api/v1/workflows                 - List workflows
GET    /api/v1/workflows/{id}            - Get workflow
PUT    /api/v1/workflows/{id}            - Update workflow
DELETE /api/v1/workflows/{id}            - Delete workflow
POST   /api/v1/workflows/{id}/execute    - Execute workflow
GET    /api/v1/workflows/{id}/executions - List executions
GET    /api/v1/executions/{id}           - Get execution details
POST   /api/v1/executions/{id}/cancel    - Cancel execution
```

---

### Phase 4: Create Catalog Service

**Goal**: Metadata management, data lineage, and discovery.

**New Module**: `catalog-service` (Spring Boot)

**Key Features**:
- Dataset catalog
- Pipeline catalog
- Column-level lineage
- Schema versioning
- Business glossary
- Full-text search

**Database Schema**:
```sql
CREATE TABLE datasets (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    schema JSONB,
    location VARCHAR(500),
    format VARCHAR(50),
    owner VARCHAR(100),
    tags TEXT[],
    classification VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE pipelines (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    definition JSONB,
    version INTEGER,
    created_by VARCHAR(100),
    created_at TIMESTAMP
);

CREATE TABLE lineage (
    id UUID PRIMARY KEY,
    source_dataset_id UUID REFERENCES datasets(id),
    source_column VARCHAR(255),
    target_dataset_id UUID REFERENCES datasets(id),
    target_column VARCHAR(255),
    transformation VARCHAR(100),
    pipeline_id UUID REFERENCES pipelines(id)
);

CREATE TABLE business_glossary (
    id UUID PRIMARY KEY,
    term VARCHAR(255) NOT NULL,
    definition TEXT,
    category VARCHAR(100),
    related_datasets UUID[]
);
```

**REST API**:
```
GET    /api/v1/catalog/datasets          - List datasets
GET    /api/v1/catalog/datasets/{id}     - Get dataset
POST   /api/v1/catalog/datasets          - Register dataset
GET    /api/v1/catalog/pipelines         - List pipelines
GET    /api/v1/lineage/{dataset-id}      - Get lineage
POST   /api/v1/catalog/search            - Search catalog
GET    /api/v1/glossary                  - Business glossary
```

---

### Phase 5: Create Quality Service

**Goal**: Data quality validation and profiling.

**New Module**: `quality-service` (Spring Boot)

**Key Features**:
- Quality rule definitions
- Data profiling
- Quality scoring
- Anomaly detection
- Reconciliation

**Quality Rules Types**:
```java
public enum QualityRuleType {
    COMPLETENESS,   // Null checks
    ACCURACY,       // Format validation, range checks
    CONSISTENCY,    // Cross-field validation
    TIMELINESS,     // Freshness checks
    UNIQUENESS,     // Duplicate detection
    VALIDITY,       // Business rule validation
    REFERENTIAL     // Foreign key checks
}
```

**Database Schema**:
```sql
CREATE TABLE quality_rules (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    rule_type VARCHAR(50),
    dataset_id UUID,
    column_name VARCHAR(255),
    condition TEXT,
    threshold DECIMAL,
    severity VARCHAR(20),
    enabled BOOLEAN DEFAULT true
);

CREATE TABLE quality_results (
    id UUID PRIMARY KEY,
    rule_id UUID REFERENCES quality_rules(id),
    execution_id UUID,
    passed BOOLEAN,
    score DECIMAL,
    violations_count INTEGER,
    checked_at TIMESTAMP
);

CREATE TABLE data_profiles (
    id UUID PRIMARY KEY,
    dataset_id UUID,
    column_name VARCHAR(255),
    row_count BIGINT,
    null_count BIGINT,
    distinct_count BIGINT,
    min_value TEXT,
    max_value TEXT,
    avg_value DECIMAL,
    std_dev DECIMAL,
    profiled_at TIMESTAMP
);
```

**REST API**:
```
POST   /api/v1/quality/rules             - Create rule
GET    /api/v1/quality/rules             - List rules
POST   /api/v1/quality/validate          - Validate dataset
GET    /api/v1/quality/profile/{id}      - Profile dataset
GET    /api/v1/quality/scores            - Quality scores
POST   /api/v1/quality/reconcile         - Reconcile datasets
```

---

### Phase 6: Create Governance Service

**Goal**: Security, access control, audit, and compliance.

**New Module**: `governance-service` (Spring Boot + Spring Security)

**Key Features**:
- Authentication (OAuth2, SAML, LDAP)
- Authorization (RBAC)
- Audit logging
- Encryption & key management
- Compliance reporting

**Database Schema**:
```sql
CREATE TABLE users (
    id UUID PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255),
    full_name VARCHAR(255),
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP
);

CREATE TABLE roles (
    id UUID PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE permissions (
    id UUID PRIMARY KEY,
    resource_type VARCHAR(50),
    resource_id UUID,
    action VARCHAR(50)
);

CREATE TABLE user_roles (
    user_id UUID REFERENCES users(id),
    role_id UUID REFERENCES roles(id),
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE role_permissions (
    role_id UUID REFERENCES roles(id),
    permission_id UUID REFERENCES permissions(id),
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    action VARCHAR(100),
    resource_type VARCHAR(50),
    resource_id UUID,
    details JSONB,
    ip_address INET,
    timestamp TIMESTAMP
);

CREATE TABLE data_classification (
    id UUID PRIMARY KEY,
    dataset_id UUID,
    column_name VARCHAR(255),
    classification VARCHAR(50),  -- PII, SENSITIVE, PUBLIC, CONFIDENTIAL
    encryption_required BOOLEAN DEFAULT false
);
```

**REST API**:
```
POST   /api/v1/auth/login                - Login
POST   /api/v1/auth/logout               - Logout
GET    /api/v1/users/{id}/permissions    - Get permissions
POST   /api/v1/users                     - Create user
GET    /api/v1/roles                     - List roles
POST   /api/v1/roles                     - Create role
GET    /api/v1/audit/logs                - Query audit logs
POST   /api/v1/compliance/report         - Generate report
```

---

### Phase 7: Create Monitoring Service

**Goal**: Real-time monitoring, alerting, and observability.

**New Module**: `monitoring-service` (Spring Boot)

**Key Features**:
- Metrics collection
- Real-time dashboards
- Alert management
- Log aggregation
- Distributed tracing

**Integration**:
- Prometheus (metrics)
- Grafana (dashboards)
- ELK Stack (logs)
- Jaeger (tracing)

**Database Schema**:
```sql
CREATE TABLE alert_rules (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    metric_name VARCHAR(255),
    condition VARCHAR(100),
    threshold DECIMAL,
    duration_seconds INTEGER,
    severity VARCHAR(20),
    channels TEXT[],  -- email, slack, pagerduty
    enabled BOOLEAN DEFAULT true
);

CREATE TABLE alerts (
    id UUID PRIMARY KEY,
    rule_id UUID REFERENCES alert_rules(id),
    status VARCHAR(50),  -- FIRING, RESOLVED
    fired_at TIMESTAMP,
    resolved_at TIMESTAMP,
    message TEXT
);

CREATE TABLE metrics (
    id UUID PRIMARY KEY,
    pipeline_id UUID,
    execution_id UUID,
    metric_name VARCHAR(255),
    metric_value DECIMAL,
    timestamp TIMESTAMP
);
```

**REST API**:
```
GET    /api/v1/metrics/pipelines/{id}    - Get metrics
POST   /api/v1/alerts/rules              - Create alert rule
GET    /api/v1/alerts                    - List alerts
GET    /api/v1/monitoring/dashboards     - List dashboards
POST   /api/v1/logs/query                - Query logs
```

---

### Phase 8: Create UI Designer

**Goal**: Visual, drag-and-drop pipeline builder.

**New Module**: `ui-designer` (React + TypeScript)

**Technology Stack**:
```
Frontend:
- React 18
- TypeScript
- Redux Toolkit (state management)
- React Flow (visual canvas)
- Monaco Editor (expression editor)
- Material-UI (component library)
- Axios (API client)
- Recharts (data visualization)
```

**Key Features**:
1. **Visual Canvas**
   - Drag-and-drop transformations
   - Connection lines between nodes
   - Zoom and pan
   - Grid snapping

2. **Transformation Configuration**
   - Dynamic forms based on transformation metadata
   - Expression builder for conditions
   - Data type validation
   - Auto-complete

3. **Data Preview**
   - Inline data preview at each step
   - Schema visualization
   - Sample data generation
   - Statistical profiling

4. **Pipeline Templates**
   - Pre-built templates (fraud detection, loan processing, etc.)
   - Template marketplace
   - Save custom templates

**Component Structure**:
```
ui-designer/
├── src/
│   ├── components/
│   │   ├── Canvas/
│   │   │   ├── PipelineCanvas.tsx
│   │   │   ├── TransformationNode.tsx
│   │   │   └── ConnectionEdge.tsx
│   │   ├── Sidebar/
│   │   │   ├── TransformationPalette.tsx
│   │   │   └── PropertyPanel.tsx
│   │   ├── ExpressionBuilder/
│   │   │   ├── ExpressionEditor.tsx
│   │   │   └── FunctionLibrary.tsx
│   │   └── DataPreview/
│   │       ├── DataGrid.tsx
│   │       └── SchemaViewer.tsx
│   ├── services/
│   │   ├── api.ts
│   │   └── pipelineService.ts
│   ├── store/
│   │   ├── pipelineSlice.ts
│   │   └── transformationSlice.ts
│   └── App.tsx
```

---

## Implementation Timeline

### Quarter 1 (Months 1-3): Foundation
- ✅ Week 1-2: Extract connectors module
- ✅ Week 3-4: Create SDK client library
- Week 5-8: Build orchestrator service
- Week 9-12: Build catalog service

### Quarter 2 (Months 4-6): Enterprise Features
- Week 13-16: Build quality service
- Week 17-20: Build governance service
- Week 21-24: Build monitoring service

### Quarter 3 (Months 7-9): UI & Advanced Features
- Week 25-32: Build visual UI designer
- Week 33-36: Build admin console

### Quarter 4 (Months 10-12): Integration & Polish
- Week 37-40: Build CLI and Python SDK
- Week 41-44: Integration testing
- Week 45-48: Documentation and launch preparation

---

## Immediate Next Steps (Week 1)

### 1. Extract Connectors Module
```bash
# Create new module
mkdir -p pipeline-sdk/sdk-connectors/src/main/java/com/enterprise/pipeline/connector
mkdir -p pipeline-sdk/sdk-connectors/src/main/resources/META-INF/services

# Move connector files
mv pipeline-sdk/sdk-transformations/src/main/java/com/enterprise/pipeline/connector/* \
   pipeline-sdk/sdk-connectors/src/main/java/com/enterprise/pipeline/connector/

# Move service registrations
mv pipeline-sdk/sdk-transformations/src/main/resources/META-INF/services/com.enterprise.pipeline.api.Source \
   pipeline-sdk/sdk-connectors/src/main/resources/META-INF/services/

mv pipeline-sdk/sdk-transformations/src/main/resources/META-INF/services/com.enterprise.pipeline.api.Sink \
   pipeline-sdk/sdk-connectors/src/main/resources/META-INF/services/
```

### 2. Create sdk-connectors/pom.xml
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project>
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.enterprise.pipeline</groupId>
        <artifactId>pipeline-sdk</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>

    <artifactId>sdk-connectors</artifactId>
    <name>SDK Connectors</name>

    <dependencies>
        <dependency>
            <groupId>com.enterprise.pipeline</groupId>
            <artifactId>sdk-api</artifactId>
        </dependency>

        <!-- AWS SDK -->
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>s3</artifactId>
        </dependency>

        <!-- Kafka -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
        </dependency>
    </dependencies>
</project>
```

### 3. Update pipeline-sdk/pom.xml
Add `<module>sdk-connectors</module>` to modules list.

### 4. Update dependencies
Projects using connectors should add:
```xml
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-connectors</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## Testing Strategy

### Unit Tests
- Each module has >80% code coverage
- Mock external dependencies
- Test edge cases and error handling

### Integration Tests
- Test inter-module communication
- Test with real databases (Testcontainers)
- Test with embedded Kafka

### End-to-End Tests
- Complete pipeline execution tests
- UI automation tests (Selenium/Playwright)
- Performance benchmarks

### Load Tests
- JMeter/Gatling for API load testing
- Spark performance benchmarks
- Database performance tests

---

## Migration Guide for Existing Users

### Breaking Changes
None - all changes are additive. Existing code continues to work.

### Recommended Upgrades
```xml
<!-- Before: Single dependency -->
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-transformations</artifactId>
</dependency>

<!-- After: Separate concerns -->
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-transformations</artifactId>
</dependency>
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-connectors</artifactId>
</dependency>
```

---

## Documentation Updates

### New Documentation Needed
1. Architecture overview (ARCHITECTURE.md) ✅ DONE
2. Module dependency graph
3. API documentation (OpenAPI/Swagger)
4. User guide for each service
5. Developer guide for custom extensions
6. Deployment guide (Docker, Kubernetes)
7. Migration guide
8. Troubleshooting guide

---

## Success Metrics

### Technical Metrics
- Module independence verified (can use SDK without platform)
- API response time <100ms (p95)
- Pipeline execution throughput >1M records/sec
- 99.9% uptime SLA
- Zero data loss guarantee

### Business Metrics
- Time to create pipeline <5 minutes (vs hours of coding)
- Developer productivity up 10x
- Data quality incidents down 80%
- Audit preparation time down 90%
- TCO reduction 50%

---

## Conclusion

This refactoring plan transforms the platform from a simple SDK to a world-class
enterprise data platform while maintaining backward compatibility and SDK independence.

The modular architecture allows users to:
1. Use just the SDK (standalone)
2. Use SDK + specific services (e.g., orchestrator only)
3. Use the full platform (all services)

Each module is independently deployable, testable, and scalable.
