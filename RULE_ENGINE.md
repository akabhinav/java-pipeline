# Rule Engine Architecture - UI-Based Rule Definition

## Overview

The Rule Engine allows business users to define rules through a visual UI without writing code. Rules can be validation rules (data quality), transformation rules (data manipulation), business rules (banking logic), or filter rules (row selection).

---

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                    UI Layer (React)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Rule Builder │  │ Expression   │  │ Test & Debug │     │
│  │   Canvas     │  │   Editor     │  │    Panel     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────┬────────────────────────────────────┘
                         │ JSON Rule Definition
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                  Rule Engine Service                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Rule Parser  │→ │ Rule Validator│→│ Rule Executor│     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────┬────────────────────────────────────┘
                         │ Spark SQL / DataFrame API
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   Apache Spark Dataset                      │
└─────────────────────────────────────────────────────────────┘
```

---

## Rule Types

### 1. Validation Rules
Check data quality and raise alerts on violations.

```json
{
  "ruleType": "VALIDATION",
  "ruleName": "validate_loan_amount",
  "description": "Loan amount must be between $1,000 and $1,000,000",
  "severity": "ERROR",
  "conditions": [
    {
      "field": "loan_amount",
      "operator": "BETWEEN",
      "values": [1000, 1000000]
    }
  ],
  "action": "FLAG_VIOLATION",
  "outputColumn": "loan_amount_valid"
}
```

### 2. Transformation Rules
Transform data based on conditions.

```json
{
  "ruleType": "TRANSFORMATION",
  "ruleName": "calculate_emi",
  "description": "Calculate monthly EMI for loans",
  "expression": {
    "formula": "P * r * (1 + r)^n / ((1 + r)^n - 1)",
    "variables": {
      "P": "loan_amount",
      "r": "monthly_interest_rate",
      "n": "tenure_months"
    }
  },
  "outputColumn": "monthly_emi"
}
```

### 3. Business Rules
Complex banking business logic.

```json
{
  "ruleType": "BUSINESS",
  "ruleName": "loan_approval_decision",
  "description": "Approve loan based on credit score and DTI",
  "conditions": [
    {
      "type": "AND",
      "rules": [
        {
          "field": "credit_score",
          "operator": "GREATER_THAN_OR_EQUAL",
          "value": 650
        },
        {
          "field": "debt_to_income_ratio",
          "operator": "LESS_THAN",
          "value": 0.43
        },
        {
          "field": "employment_status",
          "operator": "IN",
          "values": ["FULL_TIME", "SELF_EMPLOYED"]
        }
      ]
    }
  ],
  "actions": [
    {
      "field": "loan_status",
      "value": "APPROVED"
    },
    {
      "field": "approval_reason",
      "value": "Meets credit and DTI requirements"
    }
  ],
  "elseActions": [
    {
      "field": "loan_status",
      "value": "REJECTED"
    }
  ]
}
```

### 4. Filter Rules
Select rows based on conditions.

```json
{
  "ruleType": "FILTER",
  "ruleName": "high_risk_transactions",
  "description": "Filter high-risk transactions for review",
  "conditions": [
    {
      "type": "OR",
      "rules": [
        {
          "field": "amount",
          "operator": "GREATER_THAN",
          "value": 10000
        },
        {
          "field": "country",
          "operator": "IN",
          "values": ["NG", "ZA", "KE"]
        },
        {
          "field": "merchant_category",
          "operator": "EQUALS",
          "value": "GAMBLING"
        }
      ]
    }
  ]
}
```

---

## Rule Definition Schema

### Complete Rule Schema

```typescript
interface Rule {
  // Metadata
  ruleId?: string;
  ruleName: string;
  description?: string;
  ruleType: "VALIDATION" | "TRANSFORMATION" | "BUSINESS" | "FILTER";
  enabled: boolean;
  priority?: number;
  tags?: string[];

  // Applicability
  applyTo?: {
    datasets?: string[];
    columns?: string[];
    conditions?: Condition[];
  };

  // Rule definition
  conditions?: Condition | ConditionGroup;
  expression?: Expression;

  // Actions
  actions?: Action[];
  elseActions?: Action[];

  // Output
  outputColumn?: string;
  outputType?: string;

  // Quality
  severity?: "INFO" | "WARNING" | "ERROR" | "CRITICAL";

  // Metadata
  createdBy?: string;
  createdAt?: string;
  modifiedBy?: string;
  modifiedAt?: string;
  version?: number;
}

interface Condition {
  field: string;
  operator: Operator;
  value?: any;
  values?: any[];
  compareField?: string;
}

interface ConditionGroup {
  type: "AND" | "OR" | "NOT";
  rules: (Condition | ConditionGroup)[];
}

type Operator =
  | "EQUALS"
  | "NOT_EQUALS"
  | "GREATER_THAN"
  | "GREATER_THAN_OR_EQUAL"
  | "LESS_THAN"
  | "LESS_THAN_OR_EQUAL"
  | "BETWEEN"
  | "IN"
  | "NOT_IN"
  | "CONTAINS"
  | "STARTS_WITH"
  | "ENDS_WITH"
  | "MATCHES_REGEX"
  | "IS_NULL"
  | "IS_NOT_NULL"
  | "IS_EMPTY"
  | "IS_NOT_EMPTY";

interface Expression {
  formula: string;
  variables?: Record<string, string>;
  functions?: string[];
}

interface Action {
  field: string;
  value?: any;
  expression?: string;
}
```

---

## Banking Rule Examples

### Example 1: Credit Card Validation

```json
{
  "ruleName": "validate_credit_card",
  "ruleType": "VALIDATION",
  "description": "Validate credit card number using Luhn algorithm",
  "conditions": [
    {
      "field": "credit_card_number",
      "operator": "MATCHES_REGEX",
      "value": "^[0-9]{13,19}$"
    }
  ],
  "customValidation": {
    "function": "luhn_check",
    "field": "credit_card_number"
  },
  "severity": "ERROR",
  "outputColumn": "credit_card_valid"
}
```

### Example 2: Interest Rate Calculation

```json
{
  "ruleName": "calculate_interest_rate",
  "ruleType": "TRANSFORMATION",
  "description": "Determine interest rate based on credit score",
  "conditions": [
    {
      "type": "CASE",
      "cases": [
        {
          "when": {
            "field": "credit_score",
            "operator": "GREATER_THAN_OR_EQUAL",
            "value": 750
          },
          "then": {
            "field": "interest_rate",
            "value": 0.05
          }
        },
        {
          "when": {
            "field": "credit_score",
            "operator": "BETWEEN",
            "values": [650, 749]
          },
          "then": {
            "field": "interest_rate",
            "value": 0.07
          }
        },
        {
          "when": {
            "field": "credit_score",
            "operator": "BETWEEN",
            "values": [550, 649]
          },
          "then": {
            "field": "interest_rate",
            "value": 0.10
          }
        }
      ],
      "else": {
        "field": "interest_rate",
        "value": 0.15
      }
    }
  ],
  "outputColumn": "interest_rate"
}
```

### Example 3: Fraud Detection Rule

```json
{
  "ruleName": "detect_suspicious_transactions",
  "ruleType": "BUSINESS",
  "description": "Flag suspicious transactions for review",
  "conditions": {
    "type": "OR",
    "rules": [
      {
        "type": "AND",
        "rules": [
          {
            "field": "amount",
            "operator": "GREATER_THAN",
            "value": 5000
          },
          {
            "field": "transaction_time",
            "operator": "BETWEEN",
            "values": ["22:00:00", "06:00:00"]
          }
        ]
      },
      {
        "field": "velocity_count",
        "operator": "GREATER_THAN",
        "value": 10,
        "description": "More than 10 transactions in 1 hour"
      },
      {
        "type": "AND",
        "rules": [
          {
            "field": "merchant_country",
            "operator": "NOT_EQUALS",
            "compareField": "customer_country"
          },
          {
            "field": "amount",
            "operator": "GREATER_THAN",
            "value": 1000
          }
        ]
      }
    ]
  },
  "actions": [
    {
      "field": "fraud_flag",
      "value": true
    },
    {
      "field": "fraud_score",
      "expression": "CASE WHEN amount > 10000 THEN 90 WHEN velocity_count > 15 THEN 85 ELSE 70 END"
    },
    {
      "field": "review_required",
      "value": true
    }
  ]
}
```

### Example 4: KYC Compliance Check

```json
{
  "ruleName": "kyc_compliance_check",
  "ruleType": "VALIDATION",
  "description": "Ensure KYC documents are complete and valid",
  "conditions": {
    "type": "AND",
    "rules": [
      {
        "field": "id_document",
        "operator": "IS_NOT_NULL"
      },
      {
        "field": "address_proof",
        "operator": "IS_NOT_NULL"
      },
      {
        "field": "document_expiry_date",
        "operator": "GREATER_THAN",
        "value": "CURRENT_DATE"
      },
      {
        "type": "OR",
        "rules": [
          {
            "field": "id_type",
            "operator": "EQUALS",
            "value": "PASSPORT"
          },
          {
            "field": "id_type",
            "operator": "EQUALS",
            "value": "DRIVERS_LICENSE"
          },
          {
            "field": "id_type",
            "operator": "EQUALS",
            "value": "NATIONAL_ID"
          }
        ]
      }
    ]
  },
  "severity": "CRITICAL",
  "outputColumn": "kyc_compliant"
}
```

### Example 5: Loan Eligibility

```json
{
  "ruleName": "loan_eligibility",
  "ruleType": "BUSINESS",
  "description": "Determine loan eligibility",
  "conditions": {
    "type": "AND",
    "rules": [
      {
        "field": "age",
        "operator": "BETWEEN",
        "values": [21, 65]
      },
      {
        "field": "credit_score",
        "operator": "GREATER_THAN_OR_EQUAL",
        "value": 600
      },
      {
        "field": "monthly_income",
        "operator": "GREATER_THAN_OR_EQUAL",
        "value": 3000
      },
      {
        "field": "employment_type",
        "operator": "IN",
        "values": ["SALARIED", "SELF_EMPLOYED", "BUSINESS_OWNER"]
      },
      {
        "field": "bankruptcy_flag",
        "operator": "EQUALS",
        "value": false
      }
    ]
  },
  "actions": [
    {
      "field": "eligible",
      "value": true
    },
    {
      "field": "max_loan_amount",
      "expression": "monthly_income * 60"
    }
  ],
  "elseActions": [
    {
      "field": "eligible",
      "value": false
    },
    {
      "field": "rejection_reason",
      "expression": "CASE WHEN credit_score < 600 THEN 'Low credit score' WHEN age < 21 THEN 'Below minimum age' ELSE 'Does not meet criteria' END"
    }
  ]
}
```

---

## Rule Functions Library

Pre-built functions available in expressions:

### Mathematical Functions
- `ABS(x)` - Absolute value
- `ROUND(x, decimals)` - Round to decimals
- `CEILING(x)` - Round up
- `FLOOR(x)` - Round down
- `POWER(base, exponent)` - Exponentiation
- `SQRT(x)` - Square root
- `MOD(x, y)` - Modulo

### String Functions
- `UPPER(str)` - Convert to uppercase
- `LOWER(str)` - Convert to lowercase
- `TRIM(str)` - Remove whitespace
- `SUBSTRING(str, start, length)` - Extract substring
- `CONCAT(str1, str2, ...)` - Concatenate strings
- `REPLACE(str, search, replace)` - Replace text
- `LENGTH(str)` - String length

### Date Functions
- `CURRENT_DATE()` - Current date
- `CURRENT_TIMESTAMP()` - Current timestamp
- `DATE_ADD(date, days)` - Add days to date
- `DATE_DIFF(date1, date2)` - Difference in days
- `YEAR(date)` - Extract year
- `MONTH(date)` - Extract month
- `DAY(date)` - Extract day

### Banking Functions
- `LUHN_CHECK(card_number)` - Validate credit card
- `CALCULATE_EMI(principal, rate, tenure)` - Calculate monthly EMI
- `COMPOUND_INTEREST(principal, rate, time, frequency)` - Compound interest
- `SIMPLE_INTEREST(principal, rate, time)` - Simple interest
- `CREDIT_SCORE_GRADE(score)` - Convert score to grade (A-F)
- `RISK_CATEGORY(score)` - Risk category (LOW, MEDIUM, HIGH)

### Aggregate Functions (for grouped data)
- `SUM(column)` - Sum
- `AVG(column)` - Average
- `MIN(column)` - Minimum
- `MAX(column)` - Maximum
- `COUNT(column)` - Count

---

## UI Design Wireframe

### Rule Builder Interface

```
┌────────────────────────────────────────────────────────────────┐
│  Rule Builder - Loan Approval Rule                    [Save]   │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Rule Name: ┌─────────────────────────────────┐               │
│             │ loan_approval_decision          │               │
│             └─────────────────────────────────┘               │
│                                                                 │
│  Description: ┌──────────────────────────────────────┐        │
│               │ Approve or reject loan based on    │        │
│               │ credit score and debt-to-income    │        │
│               └──────────────────────────────────────┘        │
│                                                                 │
│  Rule Type: ● Business Rule                                   │
│             ○ Validation Rule                                  │
│             ○ Transformation Rule                              │
│             ○ Filter Rule                                      │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│  Conditions (Drag & Drop)                                      │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  🔗 AND Group                               [+ Add] [×]   │ │
│  │  ┌────────────────────────────────────────────────────┐  │ │
│  │  │  credit_score  ≥  650                          [×] │  │ │
│  │  └────────────────────────────────────────────────────┘  │ │
│  │  ┌────────────────────────────────────────────────────┐  │ │
│  │  │  debt_to_income_ratio  <  0.43                 [×] │  │ │
│  │  └────────────────────────────────────────────────────┘  │ │
│  │  ┌────────────────────────────────────────────────────┐  │ │
│  │  │  employment_status  IN  [FULL_TIME, ...]       [×] │  │ │
│  │  └────────────────────────────────────────────────────┘  │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                 │
│  [+ Add Condition]  [+ Add Group (AND/OR)]                    │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│  Actions (When conditions are TRUE)                           │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  Set Field:  loan_status  =  "APPROVED"           [×] │   │
│  └────────────────────────────────────────────────────────┘   │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  Set Field:  approval_reason  =  "Meets criteria" [×] │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                 │
│  [+ Add Action]                                                │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│  Else Actions (When conditions are FALSE)                     │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  Set Field:  loan_status  =  "REJECTED"           [×] │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                 │
│  [+ Add Else Action]                                           │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│  Preview & Test                                                │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Test with sample data:  [Upload CSV] [Use Sample]            │
│                                                                 │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  Sample Results (10 rows):                            │   │
│  │                                                         │   │
│  │  Row 1: credit_score=700, DTI=0.35 → APPROVED  ✓     │   │
│  │  Row 2: credit_score=580, DTI=0.40 → REJECTED  ✓     │   │
│  │  Row 3: credit_score=720, DTI=0.50 → REJECTED  ✓     │   │
│  │  ...                                                   │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

### Expression Builder

```
┌────────────────────────────────────────────────────────────────┐
│  Expression Builder - Calculate EMI                   [Insert] │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Formula Template:                                             │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  ● EMI Calculation                                        │ │
│  │  ○ Simple Interest                                        │ │
│  │  ○ Compound Interest                                      │ │
│  │  ○ Loan-to-Value Ratio                                    │ │
│  │  ○ Debt-to-Income Ratio                                   │ │
│  │  ○ Custom Expression                                      │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Formula:                                                      │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  P * r * (1 + r)^n / ((1 + r)^n - 1)                     │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Variables:                                                    │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  P  →  [loan_amount       ▼]                             │ │
│  │  r  →  [monthly_rate      ▼]  (annual_rate / 12)        │ │
│  │  n  →  [tenure_months     ▼]                             │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Available Functions:        Available Fields:                │
│  ┌─────────────────────┐   ┌──────────────────────────────┐  │
│  │ Math                │   │ loan_amount                  │  │
│  │  - ABS()            │   │ annual_interest_rate         │  │
│  │  - ROUND()          │   │ tenure_months                │  │
│  │  - POWER()          │   │ credit_score                 │  │
│  │                     │   │ monthly_income               │  │
│  │ String              │   │ employment_type              │  │
│  │  - CONCAT()         │   │ ...                          │  │
│  │  - UPPER()          │   │                              │  │
│  │                     │   │                              │  │
│  │ Date                │   │                              │  │
│  │  - CURRENT_DATE()   │   │                              │  │
│  │  - DATE_DIFF()      │   │                              │  │
│  └─────────────────────┘   └──────────────────────────────┘  │
│                                                                 │
│  Test Expression:                                              │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  loan_amount = 100000                                     │ │
│  │  annual_rate = 8.5                                        │ │
│  │  tenure = 60 months                                       │ │
│  │                                                            │ │
│  │  Result: ₹2,055.48/month                          ✓      │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

---

## Rule Templates (Pre-built)

### Banking Templates

1. **Loan Approval**
   - Credit score + DTI check
   - Employment verification
   - Age limits
   - Income requirements

2. **Fraud Detection**
   - Amount thresholds
   - Geographic risk
   - Velocity checks
   - Time-based patterns

3. **KYC Compliance**
   - Document verification
   - Age verification
   - Address validation
   - ID expiry check

4. **Credit Scoring**
   - Payment history
   - Credit utilization
   - Account age
   - Inquiry impact

5. **Risk Assessment**
   - Debt burden
   - Collateral value
   - Industry risk
   - Market conditions

6. **Regulatory Compliance**
   - AML checks
   - Sanctions screening
   - PEP (Politically Exposed Person) check
   - Large transaction reporting

---

## Storage & Execution

### Rule Storage (Database)

```sql
CREATE TABLE rules (
    rule_id UUID PRIMARY KEY,
    rule_name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    rule_type VARCHAR(50) NOT NULL,
    rule_definition JSONB NOT NULL,
    enabled BOOLEAN DEFAULT true,
    priority INTEGER DEFAULT 0,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_at TIMESTAMP,
    version INTEGER DEFAULT 1,
    tags TEXT[]
);

CREATE TABLE rule_executions (
    execution_id UUID PRIMARY KEY,
    rule_id UUID REFERENCES rules(rule_id),
    dataset_id UUID,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rows_processed BIGINT,
    rows_passed BIGINT,
    rows_failed BIGINT,
    execution_duration_ms BIGINT,
    status VARCHAR(50),
    error_message TEXT
);

CREATE TABLE rule_violations (
    violation_id UUID PRIMARY KEY,
    execution_id UUID REFERENCES rule_executions(execution_id),
    rule_id UUID REFERENCES rules(rule_id),
    row_id VARCHAR(255),
    violation_details JSONB,
    severity VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Rule Execution Flow

```
1. User defines rule in UI
   ↓
2. UI sends JSON to backend
   ↓
3. Backend validates rule
   ↓
4. Rule stored in database
   ↓
5. User applies rule to dataset
   ↓
6. Rule engine converts JSON to Spark operations
   ↓
7. Spark executes rule on dataset
   ↓
8. Results returned (violations, transformations, etc.)
   ↓
9. Results stored in database
   ↓
10. UI displays results to user
```

---

## Next Steps

1. ✅ Design complete rule schema
2. ⏳ Implement rule parser (JSON → Spark)
3. ⏳ Implement rule executor
4. ⏳ Create rule templates
5. ⏳ Build UI components (React)
6. ⏳ Add rule testing framework
7. ⏳ Documentation and examples
