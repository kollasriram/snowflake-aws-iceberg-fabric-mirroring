# Comprehensive End-to-End Architecture & Step-by-Step Implementation Runbook: Mirroring Snowflake on Google Cloud (GCP) & Google Cloud Storage (GCS) Apache Iceberg Tables into Microsoft Fabric

**Document Type:** Technical Implementation Runbook & Architecture Guide  
**Scenario:** Replicating Managed Tables & Apache Iceberg Tables from a Snowflake Environment on Google Cloud Platform (`<gcp-region>`, e.g., `us-central1`) and Google Cloud Storage (GCS) into Microsoft Fabric, including Automated Direct Lake Semantic Model Generation (Single & Composite Key Relationships, Descriptions & Synonyms).  
**Security Note:** All actual project IDs, account locators, bucket names, service account emails, HMAC keys, IP addresses, GUIDs, and credentials in this document are masked with standardized placeholders (`<...>`).

> **Important Note — Google Cloud Free Trial Organization Policies (`iam.disableServiceAccountKeyCreation`):**
> * **Snowflake External Volume to GCS (`STORAGE_PROVIDER = 'GCS'`):** Uses a Snowflake-generated GCP Service Account (`STORAGE_GCP_SERVICE_ACCOUNT`, e.g., `<snowflake-gcp-service-account>@gcpuscentral1-1dfa.iam.gserviceaccount.com`) granted `roles/storage.admin` on your GCS bucket. Because this uses cross-project IAM principal authorization rather than external service account keys, **Snowflake-Managed Iceberg tables (`CREATE ICEBERG TABLE`) work immediately in GCP Free Trial and enterprise accounts**.
> * **Microsoft Fabric OneLake Connection to GCS (`GoogleCloudStorage`):** Uses a **Google Cloud Storage Interoperability XML API HMAC Key & Secret** (`GOOG1...`) associated with a GCP Service Account granted `storage.objects.get` and `storage.objects.list` (`roles/storage.objectViewer`).
> * **If using a brand-new GCP Free Trial Organization:** Google Cloud creates a default Organization Policy constraint (`iam.disableServiceAccountKeyCreation = Enforced`) that disables the **Create a key for a service account** button in Cloud Storage $\rightarrow$ Settings $\rightarrow$ Interoperability. To enable Service Account HMAC key creation in a new GCP organization, grant yourself **Organization Policy Administrator** (`roles/orgpolicy.policyAdmin`) at the Organization node and override `iam.disableServiceAccountKeyCreation` to **Off** on your project (documented step-by-step in **Step 6** below).

---

## Table of Contents

1. [Executive Summary & Key Architecture Decisions (Snowflake on GCP + GCS)](#1-executive-summary--key-architecture-decisions-snowflake-on-gcp--gcs)
2. [Recommended Architecture: Snowflake on GCP & GCS to Microsoft Fabric](#2-recommended-architecture-snowflake-on-gcp--gcs-to-microsoft-fabric)
   - [2.1 Architecture at a Glance](#21-architecture-at-a-glance)
   - [2.2 How the Two Read Paths Work (Snowflake Catalog vs. Direct GCS XML API)](#22-how-the-two-read-paths-work-snowflake-catalog-vs-direct-gcs-xml-api)
   - [2.3 Public / Cloud Connectivity vs. Private Connectivity (OPDG + Private Google Access / PSC)](#23-public--cloud-connectivity-vs-private-connectivity-opdg--private-google-access--psc)
3. [Placeholder & Parameter Reference Table (Masked Inventory)](#3-placeholder--parameter-reference-table-masked-inventory)
4. [Complete Step-by-Step Setup Instructions (Every Click, SQL Query & REST API Call)](#4-complete-step-by-step-setup-instructions-every-click-sql-query--rest-api-call)
   - [Step 1: Verify Snowflake on GCP Region, Edition & Inline Unload Parameter](#step-1-verify-snowflake-on-gcp-region-edition--inline-unload-parameter)
   - [Step 2: Create the Snowflake External Volume for GCS & Retrieve the Snowflake GCP Service Account](#step-2-create-the-snowflake-external-volume-for-gcs--retrieve-the-snowflake-gcp-service-account)
   - [Step 3: Create the Google Cloud Storage (GCS) Bucket & Grant Access to Snowflake](#step-3-create-the-google-cloud-storage-gcs-bucket--grant-access-to-snowflake)
   - [Step 4: Verify External Volume & Create Multi-Table Schema (Managed + GCS Iceberg + Single & Composite Keys)](#step-4-verify-external-volume--create-multi-table-schema-managed--gcs-iceberg--single--composite-keys)
   - [Step 5: Create the GCP Service Account for Microsoft Fabric GCS Read Access](#step-5-create-the-gcp-service-account-for-microsoft-fabric-gcs-read-access)
   - [Step 6: Generate the GCS Interoperability HMAC Key & Secret (Including Org Policy Override if Needed)](#step-6-generate-the-gcs-interoperability-hmac-key--secret-including-org-policy-override-if-needed)
   - [Step 7: Configure Network Security & Optional On-Premises Data Gateway (OPDG)](#step-7-configure-network-security--optional-on-premises-data-gateway-opdg)
   - [Step 8: Create Both Connections in Microsoft Fabric (Snowflake + Google Cloud Storage)](#step-8-create-both-connections-in-microsoft-fabric-snowflake--google-cloud-storage)
   - [Step 9: Create the Mirrored Snowflake Database & OneLake GCS Iceberg Shortcut](#step-9-create-the-mirrored-snowflake-database--onelake-gcs-iceberg-shortcut)
   - [Step 10: Automate Direct Lake Semantic Model Creation (OneLake-Scoped Tables, Single & Composite Keys, Descriptions & Synonyms)](#step-10-automate-direct-lake-semantic-model-creation-onelake-scoped-tables-single--composite-keys-descriptions--synonyms)
   - [Step 11: Validate with Live DAX Queries & Enable OneLake Shortcut Caching](#step-11-validate-with-live-dax-queries--enable-onelake-shortcut-caching)
5. [End-to-End Python Automation Script (`deploy_gcp_snowflake_fabric_poc.py`)](#5-end-to-end-python-automation-script-deploy_gcp_snowflake_fabric_pocpy)
6. [Troubleshooting Matrix & Cost-Control Checklist](#6-troubleshooting-matrix--cost-control-checklist)
7. [Official Public Documentation References](#7-official-public-documentation-references)

---

## 1. Executive Summary & Key Architecture Decisions (Snowflake on GCP + GCS)

| # | Architecture Question | Complete Technical Answer |
|---|---|---|
| **1** | **How does Snowflake on GCP authenticate to Google Cloud Storage (GCS) for Iceberg tables compared to AWS S3?** | Unlike AWS (where you create an IAM Role and trust policy with an External ID), on **Google Cloud** you run `CREATE EXTERNAL VOLUME ... STORAGE_PROVIDER = 'GCS'` first, then run `DESC EXTERNAL VOLUME` to retrieve the Snowflake-provisioned **GCP Service Account email** (`STORAGE_GCP_SERVICE_ACCOUNT`, e.g., `<snowflake-gcp-sa>@gcpuscentral1-1dfa.iam.gserviceaccount.com`). You then grant that principal **`Storage Admin` (`roles/storage.admin`)** directly on your GCS bucket. |
| **2** | **How does Microsoft Fabric OneLake authenticate to Google Cloud Storage (GCS) to read Iceberg files?** | Per Microsoft's official [Create a Google Cloud Storage (GCS) shortcut](https://learn.microsoft.com/en-us/fabric/onelake/create-gcs-shortcut) documentation, Fabric uses the **GCS XML Interoperability API** (`https://<gcs-iceberg-bucket>.storage.googleapis.com` or `https://storage.googleapis.com`) and authenticates using an **HMAC Access ID (`GOOG1...`) and Secret** generated for a GCP Service Account that has `storage.objects.get` and `storage.objects.list` permissions (`roles/storage.objectViewer`). |
| **3** | **When is an On-Premises Data Gateway (OPDG) required for Snowflake on GCP and GCS?** | • **Public / Cloud Endpoint Mode (`ShareableCloud`):** If your Snowflake on GCP account and GCS bucket are not restricted by a private VPC Service Controls perimeter or IP firewall, Fabric connects directly using cloud connections (`connectivityType = "ShareableCloud"`), while **GCS Public Access Prevention remains `ON`**.<br/>• **Firewalled / Private VPC Mode (`OnPremisesGateway`):** If your Snowflake account has a Snowflake Network Policy or GCP Private Service Connect (PSC), or your GCS bucket enforces IP filtering / VPC Service Controls, Microsoft's [Create shortcuts to on-premises/network-restricted data](https://learn.microsoft.com/en-us/fabric/onelake/create-on-premises-shortcut) supports routing **both** the Snowflake connection and the Google Cloud Storage connection through a **Standard On-Premises Data Gateway (OPDG)** hosted on a Windows Server VM inside your GCP VPC (using Private Google Access for `storage.googleapis.com`). |
| **4** | **How are GCS Iceberg tables exposed in OneLake alongside Mirrored Snowflake Managed tables?** | 1. **Managed Tables (`ACCOUNTS_MANAGED`, `REGIONS_MANAGED`, `PRODUCTS_MANAGED`, `ORDERS_MANAGED`):** Replicated continuously into OneLake Delta format via `MirroredDatabase` (`POST /v1/workspaces/{workspaceId}/mirroredDatabases`).<br/>2. **GCS Iceberg Tables (`ORDERS_ICEBERG`):** Virtualized directly into OneLake `Tables/ORDERS_ICEBERG` via a **Google Cloud Storage OneLake Shortcut** pointing to the exact table subpath (`gcs://<gcs-iceberg-bucket>/iceberg/<iceberg-table-folder>`) discovered from `SHOW ICEBERG TABLES`. OneLake automatically translates the Iceberg V2 metadata (`.metadata.json`, `.avro` manifests, and `.parquet` files) into a native Delta Lake table consumable by SQL Analytics Endpoint, Spark, and Direct Lake! |
| **5** | **How are Snowflake relationships (including multi-column composite keys), descriptions, and synonyms replicated into a Direct Lake Semantic Model?** | Because Delta Lake files in OneLake do not carry relational constraints (`PRIMARY KEY` / `FOREIGN KEY`) or Snowflake `COMMENT` metadata into Power BI automatically, **Step 10** runs an automated Python pipeline that queries Snowflake's `SHOW TABLES`, `SHOW COLUMNS`, `SHOW PRIMARY KEYS`, and `SHOW IMPORTED KEYS` (filtered strictly to tables present in OneLake) and deploys a **Direct Lake Semantic Model** via the Fabric REST API (`TMDL` format) with single-column relationships, composite-key relationships (`CK_TENANT_PRODUCT` + `TREATAS` DAX measures), descriptions, and synonyms. |

---

## 2. Recommended Architecture: Snowflake on GCP & GCS to Microsoft Fabric

### 2.1 Architecture at a Glance

![Snowflake on GCP and Google Cloud Storage (GCS) to Microsoft Fabric](https://raw.githubusercontent.com/kollasriram/snowflake-aws-iceberg-fabric-mirroring/main/Snowflake_GCP_Iceberg_Fabric_Architecture.png)

#### Component Guide

| Box | Component | Configuration Details |
|---|---|---|
| **1. Snowflake on GCP** | Snowflake Enterprise/Standard account in `<gcp-region>` (`us-central1`) | Hosts database `<SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>`, managed tables with `CHANGE_TRACKING = TRUE`, external volume `<EXTERNAL_VOLUME_NAME>` (`STORAGE_PROVIDER = 'GCS'`), and Iceberg table `ORDERS_ICEBERG`. |
| **2. Google Cloud Storage (GCS)** | GCS bucket `<gcs-iceberg-bucket>` | Enforces **Public Access Prevention = On** and **Uniform bucket-level access**. Grants `Storage Admin` to `<snowflake-gcp-service-account>` and `Storage Object Viewer` to `<fabric-gcs-service-account>`. |
| **3. Snowflake PSC / Network Policy** | Snowflake access control | Uses GCP Private Service Connect (Business Critical) or Snowflake Network Policy IP allowlisting when firewalled. |
| **4. Private Google Access / XML API** | `https://<gcs-iceberg-bucket>.storage.googleapis.com` | Exposes the GCS XML API endpoint authenticated via Service Account HMAC Key (`Access ID` + `Secret`). |
| **5. Windows GCE / OPDG** | On-Premises Data Gateway (`<opdg-cluster-name>`) | Required when Snowflake or GCS is behind a firewall/VPC perimeter; optional (`ShareableCloud`) when endpoints allow Microsoft Fabric cloud access. |
| **6. Two Fabric Connections** | `<fabric-snowflake-gcp-connection-name>` & `<fabric-gcs-connection-name>` | Connection #1 (`Snowflake`) connects to `<snowflake-gcp-account>.snowflakecomputing.com`. Connection #2 (`GoogleCloudStorage`) connects to `https://<gcs-iceberg-bucket>.storage.googleapis.com` with HMAC credentials. |
| **7. Mirrored DB + GCS Table Shortcut** | `<fabric-mirrored-db-name>` & `<fabric-lakehouse-name>` | Replicates Snowflake Managed tables via CDC streams and virtualizes GCS Iceberg tables (`ORDERS_ICEBERG`) in OneLake `Tables/`. |
| **8. OneLake Storage** | Unified Delta + Iceberg virtualization | Converts Iceberg V2 metadata in GCS into Delta Lake format on the fly; supports 1–28 day OneLake shortcut file caching. |
| **9. Direct Lake Semantic Model** | `<fabric-semantic-model-name>` | Auto-generated via TMDL from Snowflake `SHOW PRIMARY KEYS`, `SHOW IMPORTED KEYS`, and `COMMENT` metadata (including single-column FKs, composite FKs, hierarchies, and DAX measures). |

### 2.2 How the Two Read Paths Work (Snowflake Catalog vs. Direct GCS XML API)

1. **Path 1 — Snowflake SQL & Change Tracking (`Snowflake` Connection):** Fabric connects to `<snowflake-gcp-account>.snowflakecomputing.com` using `<snowflake-mirror-user>` (`<SNOWFLAKE_MIRROR_ROLE>`) to create change-tracking streams (`Fabric_Mirror_<Table>_Stream_...`) and replicate managed tables into OneLake Delta tables.
2. **Path 2 — Direct Google Cloud Storage Reads (`GoogleCloudStorage` Connection):** OneLake reads the Apache Iceberg metadata (`*.metadata.json`, manifest lists, manifests) and Parquet data files (`*.parquet`) directly from `https://<gcs-iceberg-bucket>.storage.googleapis.com/iceberg/<iceberg-table-folder>` using the GCP Service Account HMAC key—without routing file reads through Snowflake's compute warehouse.

### 2.3 Public / Cloud Connectivity vs. Private Connectivity (OPDG + Private Google Access / PSC)

| Connectivity Mode | When to Use | Fabric Connection `connectivityType` |
|---|---|---|
| **Cloud Mode (`ShareableCloud`)** | GCS bucket has **Public Access Prevention = On** (authenticated IAM/HMAC only) and no IP/VPC perimeter block; Snowflake has no IP network policy block. | `ShareableCloud` (`gatewayId = null`) |
| **Private / Firewalled Mode (`OnPremisesGateway`)** | GCS bucket enforces **GCS IP Filtering** or **VPC Service Controls**, and/or Snowflake enforces a **Snowflake Network Policy** or **GCP Private Service Connect (PSC)**. | `OnPremisesGateway` (`gatewayId = <opdg-gateway-cluster-id>`) |

---

## 3. Placeholder & Parameter Reference Table (Masked Inventory)

| Masked Placeholder | Example Format / Value | Description |
|---|---|---|
| `<gcp-organization-id>` | `123456789012` | Google Cloud Organization ID (if applicable) |
| `<gcp-project-id>` | `project-xxxxxxxx-xxxx-xxxx-xxx` | Google Cloud Project ID hosting the GCS bucket |
| `<gcp-region>` | `us-central1` | Google Cloud region matching your Snowflake on GCP region |
| `<gcs-iceberg-bucket>` | `company-fabric-snowflake-iceberg-gcp` | GCS bucket storing Iceberg metadata and Parquet files |
| `<snowflake-gcp-service-account>` | `xxxx@gcpuscentral1-1dfa.iam.gserviceaccount.com` | Returned by `DESC EXTERNAL VOLUME` in Snowflake |
| `<fabric-gcs-service-account>` | `fabric-gcp-poc-admin@<gcp-project-id>.iam.gserviceaccount.com` | GCP Service Account used for Fabric GCS HMAC authentication |
| `<gcs-hmac-access-id>` | `GOOG1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX` | GCS Interoperability HMAC Access Key ID |
| `<gcs-hmac-secret>` | `****************************************` | GCS Interoperability HMAC Secret Key |
| `<snowflake-gcp-account>` | `orgname-acctname` | Snowflake on GCP account identifier (`<snowflake-gcp-account>.snowflakecomputing.com`) |
| `<snowflake-mirror-user>` | `SVC_FABRIC_MIRROR_USER` | Snowflake user configured in the Fabric Snowflake connection |
| `<snowflake-mirror-password>` | `********************` | Password for `<snowflake-mirror-user>` |
| `<SNOWFLAKE_MIRROR_ROLE>` | `FABRIC_MIRROR_ROLE` | Snowflake role granted `USAGE`, `CREATE STREAM`, and `SELECT` privileges |
| `<SNOWFLAKE_WAREHOUSE>` | `COMPUTE_WH` | Uppercase/exact-case Snowflake virtual warehouse name |
| `<SNOWFLAKE_DB>` | `FABRIC_ICEBERG_GCP_DB` | Uppercase/exact-case Snowflake database name |
| `<SNOWFLAKE_SCHEMA>` | `POC_SCHEMA` | Uppercase/exact-case Snowflake schema name |
| `<EXTERNAL_VOLUME_NAME>` | `EXVOL_FABRIC_ICEBERG_GCS` | Snowflake External Volume object name pointing to `gcs://<gcs-iceberg-bucket>/iceberg/` |
| `<fabric-workspace-id>` | `00000000-0000-0000-0000-000000000001` | GUID of the target Microsoft Fabric workspace |
| `<fabric-mirrored-db-id>` | `00000000-0000-0000-0000-000000000002` | GUID of the Mirrored Snowflake Database item in Fabric |
| `<fabric-lakehouse-id>` | `00000000-0000-0000-0000-000000000003` | GUID of the Fabric Lakehouse hosting the GCS Iceberg table shortcut |
| `<fabric-semantic-model-id>` | `00000000-0000-0000-0000-000000000004` | GUID of the deployed Direct Lake Semantic Model |
| `<fabric-snowflake-gcp-connection-id>` | `00000000-0000-0000-0000-000000000005` | GUID of the Fabric Snowflake connection |
| `<fabric-gcs-connection-id>` | `00000000-0000-0000-0000-000000000006` | GUID of the Fabric Google Cloud Storage connection |

---

## 4. Complete Step-by-Step Setup Instructions (Every Click, SQL Query & REST API Call)

---

### Step 1: Verify Snowflake on GCP Region, Edition & Inline Unload Parameter

Log into Snowflake as `ACCOUNTADMIN` and verify that your account is hosted on Google Cloud (`GCP_US_CENTRAL1`, etc.) and that `PREVENT_UNLOAD_TO_INLINE_URL` is `FALSE`:

```sql
USE ROLE ACCOUNTADMIN;

-- 1. Confirm GCP Region (e.g., PUBLIC.GCP_US_CENTRAL1) and Account Locator
SELECT CURRENT_REGION() AS SNOWFLAKE_REGION,
       CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR,
       CURRENT_VERSION() AS SNOWFLAKE_VERSION;

-- 2. Verify PREVENT_UNLOAD_TO_INLINE_URL is FALSE
SHOW PARAMETERS LIKE 'PREVENT_UNLOAD_TO_INLINE_URL' IN ACCOUNT;
ALTER ACCOUNT SET PREVENT_UNLOAD_TO_INLINE_URL = FALSE;

-- 3. Create or verify the Virtual Warehouse
CREATE WAREHOUSE IF NOT EXISTS <SNOWFLAKE_WAREHOUSE>
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE <SNOWFLAKE_WAREHOUSE>;
```

---

### Step 2: Create the Snowflake External Volume for GCS & Retrieve the Snowflake GCP Service Account

In Snowflake (`ACCOUNTADMIN`), create the `EXTERNAL VOLUME` with `STORAGE_PROVIDER = 'GCS'` pointing to your target GCS bucket URL (`gcs://<gcs-iceberg-bucket>/iceberg/`):

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE EXTERNAL VOLUME <EXTERNAL_VOLUME_NAME>
  STORAGE_LOCATIONS = (
    (
      NAME = 'gcs_iceberg_us_central1'
      STORAGE_PROVIDER = 'GCS'
      STORAGE_BASE_URL = 'gcs://<gcs-iceberg-bucket>/iceberg/'
    )
  )
  ALLOW_WRITES = TRUE;

-- Retrieve the Snowflake-provisioned GCP Service Account email
DESC EXTERNAL VOLUME <EXTERNAL_VOLUME_NAME>;
```

From the `STORAGE_LOCATION_1` JSON in `DESC EXTERNAL VOLUME`, copy the **`STORAGE_GCP_SERVICE_ACCOUNT`** value:
* Example: `<snowflake-gcp-service-account>` (e.g., `kpuv50000@gcpuscentral1-1dfa.iam.gserviceaccount.com`).

---

### Step 3: Create the Google Cloud Storage (GCS) Bucket & Grant Access to Snowflake

#### Option A: Via `gcloud` CLI
```bash
# 1. Create the GCS bucket with Public Access Prevention enforced and Uniform Bucket-Level Access enabled
gcloud storage buckets create gs://<gcs-iceberg-bucket> \
  --project=<gcp-project-id> \
  --location=<gcp-region> \
  --uniform-bucket-level-access \
  --public-access-prevention

# 2. Grant Storage Admin on the bucket to the Snowflake GCP Service Account from Step 2
gcloud storage buckets add-iam-policy-binding gs://<gcs-iceberg-bucket> \
  --member="serviceAccount:<snowflake-gcp-service-account>" \
  --role="roles/storage.admin"
```

#### Option B: Via Google Cloud Console UI
1. In the **Google Cloud Console**, navigate to **Cloud Storage** $\rightarrow$ **Buckets** $\rightarrow$ click **Create**.
2. Enter `<gcs-iceberg-bucket>` $\rightarrow$ choose your region (`<gcp-region>`, or `us` Multi-region) $\rightarrow$ keep **Enforce public access prevention on this bucket** checked (`ON`) and **Access control = Uniform** $\rightarrow$ click **Create**.
3. Open the bucket's **Permissions** tab $\rightarrow$ click **Grant access**.
4. Under **New principals**, paste `<snowflake-gcp-service-account>`.
5. Under **Select a role**, choose **Cloud Storage** $\rightarrow$ **Storage Admin** $\rightarrow$ click **Save**.

---

### Step 4: Verify External Volume & Create Multi-Table Schema (Managed + GCS Iceberg + Single & Composite Keys)

Now run the following SQL in Snowflake to:
1. Verify the GCS External Volume handshake (`SYSTEM$VERIFY_EXTERNAL_VOLUME`).
2. Create a 6-table schema (`ACCOUNTS_MANAGED`, `REGIONS_MANAGED`, `PRODUCTS_MANAGED`, `ORDERS_MANAGED`, the GCS-backed Apache Iceberg table **`ORDERS_ICEBERG`**, and an un-mirrored table `INTERNAL_AUDIT_LOGS_UNMIRRORED`) with single-column PK/FKs, multi-column composite PK/FKs (`(TENANT_ID, PRODUCT_CODE)`), table/column `COMMENT` descriptions, and `[SYNONYMS: ...]`.
3. Create `<SNOWFLAKE_MIRROR_ROLE>` and `<snowflake-mirror-user>` with all required mirroring privileges.

*(Important Snowflake Type Compatibility Note: In Snowflake Iceberg tables, `INT` is a 32-bit integer corresponding to `NUMBER(10,0)`. Always use `NUMBER(10,0)` on Managed table integer keys if you want compatible key widths across Managed and Iceberg tables, and use `DECIMAL(9,2)` (`precision < 10`) to avoid the known OneLake Iceberg-to-Delta type-width limitation).*

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE <SNOWFLAKE_WAREHOUSE>;

-- 1. Verify External Volume handshake with GCS
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('<EXTERNAL_VOLUME_NAME>');

-- 2. Create Database and Schema
CREATE DATABASE IF NOT EXISTS <SNOWFLAKE_DB>;
CREATE SCHEMA IF NOT EXISTS <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>;
USE SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>;

-- 3. Dimension Table #1: ACCOUNTS_MANAGED (Single-Column PK)
CREATE OR REPLACE TABLE ACCOUNTS_MANAGED (
    ACCOUNT_ID NUMBER(10,0) NOT NULL COMMENT 'Unique primary key identifier for the account [SYNONYMS: Account Number, Client ID]',
    ACCOUNT_NAME STRING COMMENT 'Official registered name of the organization [SYNONYMS: Account, Client Name, Organization Name]',
    SEGMENT STRING COMMENT 'Market tier classification of the account [SYNONYMS: Market Segment, Account Tier]',
    INDUSTRY STRING COMMENT 'Primary industry vertical of the account [SYNONYMS: Sector, Vertical, Business Industry]',
    IS_ACTIVE_FLAG STRING COMMENT 'Indicates whether the account is currently active (Yes/No) [SYNONYMS: Active Status]',
    CREATED_AT TIMESTAMP_LTZ COMMENT 'Timestamp when the account record was onboarded [SYNONYMS: Onboarding Date]',
    CONSTRAINT PK_ACCOUNTS_MANAGED PRIMARY KEY (ACCOUNT_ID) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Master account dimension table containing enterprise and commercial organizations [SYNONYMS: Accounts, Clients, Organizations]';

INSERT INTO ACCOUNTS_MANAGED VALUES
    (101, 'Account Alpha',   'Enterprise', 'Financial Services', 'Yes', CURRENT_TIMESTAMP()),
    (102, 'Account Bravo',   'Commercial', 'Retail',             'Yes', CURRENT_TIMESTAMP()),
    (103, 'Account Charlie', 'Enterprise', 'Healthcare',         'Yes', CURRENT_TIMESTAMP()),
    (104, 'Account Delta',   'MidMarket',  'Manufacturing',      'Yes', CURRENT_TIMESTAMP()),
    (105, 'Account Echo',    'Enterprise', 'Technology',         'No',  CURRENT_TIMESTAMP());

-- 4. Dimension Table #2: REGIONS_MANAGED (Single-Column PK + Continent > Country > Region Hierarchy)
CREATE OR REPLACE TABLE REGIONS_MANAGED (
    REGION_ID NUMBER(10,0) NOT NULL COMMENT 'Unique primary key identifier for the sales region [SYNONYMS: Region Key, Territory ID]',
    CONTINENT STRING COMMENT 'Top-level continent name for geographical rollups [SYNONYMS: Geo, Super Region]',
    COUNTRY STRING COMMENT 'Country name within the continent [SYNONYMS: Nation, Country Name]',
    REGION_NAME STRING COMMENT 'Operating sales region name [SYNONYMS: Sales Territory, Region]',
    REGION_MANAGER STRING COMMENT 'Regional lead responsible for the territory [SYNONYMS: Territory Owner, Regional Lead]',
    CONSTRAINT PK_REGIONS_MANAGED PRIMARY KEY (REGION_ID) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Geographic sales territory dimension supporting Continent > Country > Region drill-down hierarchies [SYNONYMS: Regions, Territories, Geography]';

INSERT INTO REGIONS_MANAGED VALUES
    (10, 'North America', 'United States', 'US-Central', 'Taylor Morgan'),
    (20, 'North America', 'Canada',        'CA-Central', 'Jordan Lee'),
    (30, 'Europe',        'United Kingdom','UK-South',   'Alex Carter'),
    (40, 'Europe',        'Germany',       'DE-Central', 'Samira Klein');

-- 5. Dimension Table #3: PRODUCTS_MANAGED (Multi-Column Composite PK: TENANT_ID + PRODUCT_CODE)
CREATE OR REPLACE TABLE PRODUCTS_MANAGED (
    TENANT_ID STRING NOT NULL COMMENT 'Multi-tenant partition identifier (Part 1 of Composite Key) [SYNONYMS: Tenant Code]',
    PRODUCT_CODE STRING NOT NULL COMMENT 'Catalog SKU code within the tenant (Part 2 of Composite Key) [SYNONYMS: SKU, Item Code]',
    CK_TENANT_PRODUCT STRING NOT NULL COMMENT 'Physical composite key combining TENANT_ID and PRODUCT_CODE for Direct Lake 1:M relationship binding',
    PRODUCT_NAME STRING COMMENT 'Display name of the product subscription [SYNONYMS: Product, Offering Name]',
    PRODUCT_CATEGORY STRING COMMENT 'High-level product family classification [SYNONYMS: Category, Product Family]',
    LIST_UNIT_PRICE DECIMAL(9,2) COMMENT 'Standard catalog unit price in USD [SYNONYMS: List Price, Unit Price]',
    CONSTRAINT PK_PRODUCTS_COMPOSITE PRIMARY KEY (TENANT_ID, PRODUCT_CODE) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Product catalog dimension keyed by a multi-column composite primary key (TENANT_ID, PRODUCT_CODE) [SYNONYMS: Products, Catalog, SKUs]';

INSERT INTO PRODUCTS_MANAGED VALUES
    ('T01', 'PRD-100', 'T01|PRD-100', 'Analytics Platform Tier 1', 'Data Analytics',   12500.00),
    ('T01', 'PRD-200', 'T01|PRD-200', 'Data Integration Add-On',   'Data Integration',  4300.50),
    ('T02', 'PRD-100', 'T02|PRD-100', 'Enterprise Lakehouse Pack', 'Lakehouse',        98000.00),
    ('T02', 'PRD-300', 'T02|PRD-300', 'Storage Shortcut Module',   'Storage',           7650.25),
    ('T01', 'PRD-400', 'T01|PRD-400', 'Real-Time Streaming Tier',  'Real-Time',        15400.00);

-- 6. Managed Fact Table: ORDERS_MANAGED (2 Single-Column FKs + 1 Multi-Column Composite FK)
CREATE OR REPLACE TABLE ORDERS_MANAGED (
    ORDER_ID NUMBER(10,0) NOT NULL COMMENT 'Unique transaction order identifier [SYNONYMS: Order Number, Transaction ID]',
    ACCOUNT_ID NUMBER(10,0) NOT NULL COMMENT 'Foreign key referencing ACCOUNTS_MANAGED.ACCOUNT_ID [SYNONYMS: Account Key]',
    REGION_ID NUMBER(10,0) NOT NULL COMMENT 'Foreign key referencing REGIONS_MANAGED.REGION_ID [SYNONYMS: Region Key]',
    TENANT_ID STRING NOT NULL COMMENT 'Part 1 of composite foreign key referencing PRODUCTS_MANAGED(TENANT_ID, PRODUCT_CODE)',
    PRODUCT_CODE STRING NOT NULL COMMENT 'Part 2 of composite foreign key referencing PRODUCTS_MANAGED(TENANT_ID, PRODUCT_CODE)',
    CK_TENANT_PRODUCT STRING NOT NULL COMMENT 'Physical composite foreign key (TENANT_ID|PRODUCT_CODE) linking to PRODUCTS_MANAGED.CK_TENANT_PRODUCT',
    ORDER_QUANTITY NUMBER(10,0) COMMENT 'Number of subscription units ordered [SYNONYMS: Quantity, Units]',
    ORDER_AMOUNT DECIMAL(9,2) COMMENT 'Net order revenue amount in USD [SYNONYMS: Revenue, Sales Amount, Booking Amount]',
    DISCOUNT_AMOUNT DECIMAL(9,2) COMMENT 'Promotional discount applied to the order in USD [SYNONYMS: Discount, Rebate Amount]',
    ORDER_STATUS STRING COMMENT 'Current fulfillment lifecycle status of the order [SYNONYMS: Status]',
    ORDER_DATE DATE COMMENT 'Calendar date when the order was booked [SYNONYMS: Booking Date, Order Date]',
    CONSTRAINT PK_ORDERS_MANAGED PRIMARY KEY (ORDER_ID) NOT ENFORCED,
    CONSTRAINT FK_ORDERS_ACCOUNTS FOREIGN KEY (ACCOUNT_ID) REFERENCES ACCOUNTS_MANAGED (ACCOUNT_ID) NOT ENFORCED,
    CONSTRAINT FK_ORDERS_REGIONS FOREIGN KEY (REGION_ID) REFERENCES REGIONS_MANAGED (REGION_ID) NOT ENFORCED,
    CONSTRAINT FK_ORDERS_PRODUCTS_COMPOSITE FOREIGN KEY (TENANT_ID, PRODUCT_CODE) REFERENCES PRODUCTS_MANAGED (TENANT_ID, PRODUCT_CODE) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Core sales order fact table tracking order revenue, quantities, discounts, single-column FKs, and composite-key FKs [SYNONYMS: Orders, Sales, Transactions]';

INSERT INTO ORDERS_MANAGED VALUES
    (5001, 101, 10, 'T01', 'PRD-100', 'T01|PRD-100', 2,  25000.00, 1000.00, 'COMPLETED',  CURRENT_DATE()),
    (5002, 102, 20, 'T01', 'PRD-200', 'T01|PRD-200', 1,   4300.50,    0.00, 'PROCESSING', CURRENT_DATE()),
    (5003, 103, 30, 'T02', 'PRD-100', 'T02|PRD-100', 1,  98000.00, 5000.00, 'COMPLETED',  CURRENT_DATE()),
    (5004, 104, 10, 'T02', 'PRD-300', 'T02|PRD-300', 3,  22950.75,  450.25, 'COMPLETED',  CURRENT_DATE()),
    (5005, 105, 40, 'T01', 'PRD-400', 'T01|PRD-400', 1,  15400.00,  200.00, 'SHIPPED',    CURRENT_DATE()),
    (5006, 101, 10, 'T01', 'PRD-400', 'T01|PRD-400', 2,  30800.00,  800.00, 'COMPLETED',  CURRENT_DATE());

-- 7. GCS Apache Iceberg Fact Table: ORDERS_ICEBERG (Written directly to gs://<gcs-iceberg-bucket>/iceberg/)
CREATE OR REPLACE ICEBERG TABLE ORDERS_ICEBERG (
    ORDER_ID INT COMMENT 'Unique Iceberg order identifier [SYNONYMS: Iceberg Order ID]',
    ACCOUNT_ID INT COMMENT 'Foreign key referencing ACCOUNTS_MANAGED.ACCOUNT_ID [SYNONYMS: Account Key]',
    REGION_ID INT COMMENT 'Foreign key referencing REGIONS_MANAGED.REGION_ID [SYNONYMS: Region Key]',
    TENANT_ID STRING COMMENT 'Part 1 of composite foreign key referencing PRODUCTS_MANAGED(TENANT_ID, PRODUCT_CODE)',
    PRODUCT_CODE STRING COMMENT 'Part 2 of composite foreign key referencing PRODUCTS_MANAGED(TENANT_ID, PRODUCT_CODE)',
    CK_TENANT_PRODUCT STRING COMMENT 'Physical composite foreign key (TENANT_ID|PRODUCT_CODE) linking to PRODUCTS_MANAGED.CK_TENANT_PRODUCT',
    ORDER_QUANTITY INT COMMENT 'Number of subscription units ordered [SYNONYMS: Iceberg Units]',
    ORDER_AMOUNT DECIMAL(9,2) COMMENT 'Net Iceberg order revenue amount in USD [SYNONYMS: Iceberg Revenue, Iceberg Sales]',
    DISCOUNT_AMOUNT DECIMAL(9,2) COMMENT 'Promotional discount applied to the Iceberg order in USD',
    ORDER_STATUS STRING COMMENT 'Current fulfillment status of the Iceberg order',
    ORDER_DATE DATE COMMENT 'Calendar date when the Iceberg order was booked',
    CONSTRAINT PK_ORDERS_ICEBERG PRIMARY KEY (ORDER_ID) NOT ENFORCED
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = '<EXTERNAL_VOLUME_NAME>'
  BASE_LOCATION = 'orders_iceberg/'
  CHANGE_TRACKING = TRUE
  COMMENT = 'Snowflake-Managed Apache Iceberg fact table stored in Google Cloud Storage (GCS) with single and composite FKs [SYNONYMS: Iceberg Orders, GCS Orders]';

INSERT INTO ORDERS_ICEBERG SELECT * FROM ORDERS_MANAGED;

-- Inspect the generated GCS folder path (e.g., orders_iceberg.<hash>/)
SHOW ICEBERG TABLES IN SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>;

-- 8. Un-Mirrored Table: INTERNAL_AUDIT_LOGS_UNMIRRORED (Used to verify OneLake-scoped Semantic Model filtering)
CREATE OR REPLACE TABLE INTERNAL_AUDIT_LOGS_UNMIRRORED (
    AUDIT_ID NUMBER(10,0) NOT NULL,
    ACCOUNT_ID NUMBER(10,0) NOT NULL,
    AUDIT_EVENT STRING,
    CONSTRAINT PK_AUDIT_LOGS PRIMARY KEY (AUDIT_ID) NOT ENFORCED,
    CONSTRAINT FK_AUDIT_ACCOUNTS FOREIGN KEY (ACCOUNT_ID) REFERENCES ACCOUNTS_MANAGED (ACCOUNT_ID) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Internal audit table NOT mirrored to OneLake';

-- 9. Create Mirroring Role & User and Grant Privileges
CREATE ROLE IF NOT EXISTS <SNOWFLAKE_MIRROR_ROLE>;
CREATE USER IF NOT EXISTS <snowflake-mirror-user>
  PASSWORD = '<snowflake-mirror-password>'
  DEFAULT_ROLE = <SNOWFLAKE_MIRROR_ROLE>
  DEFAULT_WAREHOUSE = <SNOWFLAKE_WAREHOUSE>
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE <SNOWFLAKE_MIRROR_ROLE> TO USER <snowflake-mirror-user>;
GRANT USAGE ON WAREHOUSE <SNOWFLAKE_WAREHOUSE> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT USAGE ON DATABASE <SNOWFLAKE_DB> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT USAGE, CREATE STREAM ON SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT SELECT ON FUTURE TABLES IN SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT USAGE ON EXTERNAL VOLUME <EXTERNAL_VOLUME_NAME> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
```

---

### Step 5: Create the GCP Service Account for Microsoft Fabric GCS Read Access

Per Microsoft's official [Create a Google Cloud Storage (GCS) shortcut](https://learn.microsoft.com/en-us/fabric/onelake/create-gcs-shortcut#authorization) documentation, create a dedicated GCP Service Account (`<fabric-gcs-service-account>`) with `storage.objects.get` and `storage.objects.list` permissions (`roles/storage.objectViewer` on `<gcs-iceberg-bucket>`):

```bash
gcloud iam service-accounts create fabric-gcp-poc-admin \
  --display-name="Fabric OneLake GCS Shortcut Service Account" \
  --project=<gcp-project-id>

gcloud storage buckets add-iam-policy-binding gs://<gcs-iceberg-bucket> \
  --member="serviceAccount:<fabric-gcs-service-account>" \
  --role="roles/storage.objectViewer"
```

---

### Step 6: Generate the GCS Interoperability HMAC Key & Secret (Including Org Policy Override if Needed)

1. **(Only if using a brand-new GCP Free Trial Organization where `Create a key for a service account` is greyed out):**
   - In Google Cloud Console, switch the project picker at the top to your **Organization node** (`<gcp-organization-id>`), navigate to **IAM & Admin** $\rightarrow$ **IAM**, click **Edit principal** next to your admin user, and add the role **Organization Policy Administrator** (`roles/orgpolicy.policyAdmin`).
   - Switch back to project `<gcp-project-id>`, navigate to **IAM & Admin** $\rightarrow$ **Organization Policies**, open **`Disable service account key creation` (`iam.disableServiceAccountKeyCreation`)**, click **Manage policy** $\rightarrow$ select **Override parent's policy** $\rightarrow$ click **Add a rule** $\rightarrow$ set **Enforcement = Off** $\rightarrow$ click **Set policy**.
2. **Generate the HMAC Key Pair:**
   - Navigate to **Cloud Storage** $\rightarrow$ **Settings** $\rightarrow$ **Interoperability** tab.
   - Under **Access keys for service accounts**, click **Create a key for a service account**.
   - Select **`<fabric-gcs-service-account>`** and click **Create key**.
   - Record the returned **Access key** (`<gcs-hmac-access-id>`, starting with `GOOG1...`) and **Secret** (`<gcs-hmac-secret>`).

---

### Step 7: Configure Network Security & Optional On-Premises Data Gateway (OPDG)

- **If using Private / Firewalled Mode:**
  1. Deploy a Windows Server 2022 VM (`e2-standard-4` or `n2-standard-8`) in your GCP VPC subnet (`<gcp-region>`) with **Private Google Access = On** so traffic to `storage.googleapis.com` (`199.36.153.8/30`) stays on Google's private network.
  2. Install and register the Microsoft **On-Premises Data Gateway (OPDG)** in Standard Mode (`<opdg-cluster-name>`) with **HTTPS mode = On** (requires zero inbound ports; outbound TCP 443 only).
  3. Apply a Snowflake Network Policy (`ALLOWED_IP_LIST`) and/or GCS IP filter allowing your OPDG VM IP and Snowflake's service account.

---

### Step 8: Create Both Connections in Microsoft Fabric (Snowflake + Google Cloud Storage)

You can create both connections either in **Manage connections and gateways** in the Fabric Portal or via the Fabric REST API (`POST https://api.fabric.microsoft.com/v1/connections`):

```powershell
$fToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ "Authorization" = "Bearer $fToken"; "Content-Type" = "application/json" }

# 1. Create Connection #1: Snowflake on GCP Connection
$sfBody = @{
    connectivityType  = "ShareableCloud" # Or "OnPremisesGateway" with gatewayId = "<opdg-gateway-cluster-id>"
    displayName       = "<fabric-snowflake-gcp-connection-name>"
    connectionDetails = @{
        type           = "Snowflake"
        creationMethod = "Snowflake.Databases"
        parameters     = @(
            @{ dataType = "Text"; name = "server"; value = "<snowflake-gcp-account>.snowflakecomputing.com" },
            @{ dataType = "Text"; name = "warehouse"; value = "<SNOWFLAKE_WAREHOUSE>" }
        )
    }
    privacyLevel      = "Organizational"
    credentialDetails = @{
        singleSignOnType     = "None"
        connectionEncryption = "NotEncrypted"
        skipTestConnection   = $false
        credentials          = @{
            credentialType = "Basic"
            username       = "<snowflake-mirror-user>"
            password       = "<snowflake-mirror-password>"
        }
    }
} | ConvertTo-Json -Depth 10

$sfConn = Invoke-RestMethod -Method Post -Uri "https://api.fabric.microsoft.com/v1/connections" -Headers $headers -Body $sfBody

# 2. Create Connection #2: Google Cloud Storage (GCS) Connection using HMAC Key & Secret
$gcsBody = @{
    connectivityType  = "ShareableCloud" # Or "OnPremisesGateway" with gatewayId = "<opdg-gateway-cluster-id>"
    displayName       = "<fabric-gcs-connection-name>"
    connectionDetails = @{
        type           = "GoogleCloudStorage"
        creationMethod = "GoogleCloudStorage.Storage"
        parameters     = @(
            @{ dataType = "Text"; name = "url"; value = "https://<gcs-iceberg-bucket>.storage.googleapis.com" }
        )
    }
    privacyLevel      = "Organizational"
    credentialDetails = @{
        singleSignOnType     = "None"
        connectionEncryption = "NotEncrypted"
        skipTestConnection   = $false
        credentials          = @{
            credentialType = "Basic"
            username       = "<gcs-hmac-access-id>"
            password       = "<gcs-hmac-secret>"
        }
    }
} | ConvertTo-Json -Depth 10

$gcsConn = Invoke-RestMethod -Method Post -Uri "https://api.fabric.microsoft.com/v1/connections" -Headers $headers -Body $gcsBody
```

---

### Step 9: Create the Mirrored Snowflake Database & OneLake GCS Iceberg Shortcut

1. **Create & Start the Mirrored Snowflake Database (`POST /v1/workspaces/{workspaceId}/mirroredDatabases`):**
   ```json
   {
     "properties": {
       "source": {
         "type": "Snowflake",
         "typeProperties": {
           "connection": "<fabric-snowflake-gcp-connection-id>",
           "database": "<SNOWFLAKE_DB>"
         }
       },
       "target": {
         "type": "MountedRelationalDatabase",
         "typeProperties": {
           "defaultSchema": "<SNOWFLAKE_SCHEMA>",
           "format": "Delta",
           "retentionInDays": 7
         }
       },
       "mountedTables": [
         { "source": { "typeProperties": { "schemaName": "<SNOWFLAKE_SCHEMA>", "tableName": "ACCOUNTS_MANAGED" } } },
         { "source": { "typeProperties": { "schemaName": "<SNOWFLAKE_SCHEMA>", "tableName": "REGIONS_MANAGED" } } },
         { "source": { "typeProperties": { "schemaName": "<SNOWFLAKE_SCHEMA>", "tableName": "PRODUCTS_MANAGED" } } },
         { "source": { "typeProperties": { "schemaName": "<SNOWFLAKE_SCHEMA>", "tableName": "ORDERS_MANAGED" } } }
       ]
     }
   }
   ```
2. **Create the OneLake GCS Table Shortcut for `ORDERS_ICEBERG` in `<fabric-lakehouse-name>`:**
   - Using the `base_location` returned by `SHOW ICEBERG TABLES` in Step 4 (e.g., `/iceberg/orders_iceberg.<hash>`), create a **Google Cloud Storage** shortcut under **`Tables/ORDERS_ICEBERG`** in your Lakehouse (`POST /v1/workspaces/{workspaceId}/items/{lakehouseId}/shortcuts`):
   ```json
   {
     "path": "Tables",
     "name": "ORDERS_ICEBERG",
     "target": {
       "googleCloudStorage": {
         "location": "https://<gcs-iceberg-bucket>.storage.googleapis.com",
         "subpath": "/iceberg/<orders-iceberg-folder>",
         "connectionId": "<fabric-gcs-connection-id>"
       }
     }
   }
   ```
   - OneLake automatically reads the Iceberg V2 metadata (`00001-...metadata.json`, `.avro` manifests, and `.parquet` files) from GCS and exposes **`dbo.ORDERS_ICEBERG`** as a queryable Delta table in the Lakehouse SQL Analytics Endpoint!

---

### Step 10: Automate Direct Lake Semantic Model Creation (OneLake-Scoped Tables, Single & Composite Keys, Descriptions & Synonyms)

Once replication is running, execute the Python automation script in **Section 5** below to:
1. Query `getTablesMirroringStatus` on `<fabric-mirrored-db-id>` so **only tables actually replicated into OneLake** are included (automatically excluding `INTERNAL_AUDIT_LOGS_UNMIRRORED`).
2. Extract table/column `COMMENT` descriptions, `[SYNONYMS: ...]`, single-column PK/FKs (`FK_ORDERS_ACCOUNTS`, `FK_ORDERS_REGIONS`), and multi-column composite PK/FKs (`FK_ORDERS_PRODUCTS_COMPOSITE` on `(TENANT_ID, PRODUCT_CODE)`) from Snowflake.
3. Generate the complete Direct Lake `TMDL` definition (including `CK_TENANT_PRODUCT` physical relationship binding and a multi-column `TREATAS` DAX measure) and deploy `<fabric-semantic-model-name>` via `POST /v1/workspaces/{workspaceId}/semanticModels`.

---

### Step 11: Validate with Live DAX Queries & Enable OneLake Shortcut Caching

1. **Query the GCS Iceberg Table (`dbo.ORDERS_ICEBERG`) via the Lakehouse SQL Analytics Endpoint:**
   ```sql
   SELECT ORDER_ID, ACCOUNT_ID, REGION_ID, TENANT_ID, PRODUCT_CODE, ORDER_QUANTITY, ORDER_AMOUNT, ORDER_STATUS
   FROM [dbo].[ORDERS_ICEBERG]
   ORDER BY ORDER_ID;
   ```
2. **Validate the Automated Direct Lake Semantic Model via DAX (`INFO.VIEW.RELATIONSHIPS()` & `SUMMARIZECOLUMNS`):**
   ```dax
   EVALUATE
   SUMMARIZECOLUMNS(
       ACCOUNTS_MANAGED[ACCOUNT_NAME],
       REGIONS_MANAGED[CONTINENT],
       REGIONS_MANAGED[REGION_NAME],
       PRODUCTS_MANAGED[TENANT_ID],
       PRODUCTS_MANAGED[PRODUCT_CODE],
       PRODUCTS_MANAGED[PRODUCT_NAME],
       "OrderCount", [Order Count],
       "Revenue_NativeCompositeRel", [Total Order Revenue],
       "Revenue_TREATASCompositeKey", [Total Order Revenue (Composite Key TREATAS)],
       "Discount", [Total Discount Amount]
   )
   ORDER BY [Revenue_NativeCompositeRel] DESC
   ```
3. **Enable OneLake Shortcut Caching:**
   - In **Workspace settings** $\rightarrow$ **OneLake** $\rightarrow$ **Cache**, toggle **Enable cache for shortcuts** to **On** (1–28 days retention) to reduce repeated cross-cloud egress from Google Cloud Storage for files `<= 1 GB`.

---

## 5. End-to-End Python Automation Script (`deploy_gcp_snowflake_fabric_poc.py`)

```python
"""
End-to-End Automation Script: Snowflake on GCP + GCS Iceberg + Fabric Mirroring + Direct Lake Semantic Model
Replace all <...> placeholders before running.
"""
import base64
import gzip
import json
import re
import time
import urllib.request
import uuid
from collections import defaultdict

WORKSPACE_ID        = "<fabric-workspace-id>"
MIRRORED_DB_ID      = "<fabric-mirrored-db-id>"
LAKEHOUSE_ID        = "<fabric-lakehouse-id>"
SEMANTIC_MODEL_NAME = "<fabric-semantic-model-name>"
FABRIC_BEARER_TOKEN = "<fabric-entra-bearer-token>"
PBI_BEARER_TOKEN    = "<powerbi-entra-bearer-token>"
GCS_CONNECTION_ID   = "<fabric-gcs-connection-id>"
GCS_BUCKET_NAME     = "<gcs-iceberg-bucket>"

SNOWFLAKE_ACCOUNT   = "<snowflake-gcp-account>"
SNOWFLAKE_USER      = "<snowflake-mirror-user>"
SNOWFLAKE_PASSWORD  = "<snowflake-mirror-password>"
SNOWFLAKE_WAREHOUSE = "<SNOWFLAKE_WAREHOUSE>"
SNOWFLAKE_DB        = "<SNOWFLAKE_DB>"
SNOWFLAKE_SCHEMA    = "<SNOWFLAKE_SCHEMA>"
EXTERNAL_VOLUME     = "<EXTERNAL_VOLUME_NAME>"

def fabric_request(method, url, body=None, token=FABRIC_BEARER_TOKEN):
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8") if body is not None else (b"" if method == "POST" else None),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8")
        return resp.status, dict(resp.headers), (json.loads(raw) if raw else {})

# 1. Authenticate to Snowflake on GCP
sf_host = f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
login_req = urllib.request.Request(
    f"{sf_host}/session/v1/login-request",
    data=json.dumps({"data": {"CLIENT_APP_ID": "PythonConnector", "CLIENT_APP_VERSION": "3.0.0",
                              "ACCOUNT_NAME": SNOWFLAKE_ACCOUNT, "LOGIN_NAME": SNOWFLAKE_USER, "PASSWORD": SNOWFLAKE_PASSWORD}}).encode("utf-8"),
    headers={"Content-Type": "application/json", "Accept": "application/json"}
)
sf_token = json.loads(urllib.request.urlopen(login_req, timeout=15).read().decode("utf-8"))["data"]["token"]

seq_id = 1
def run_sf_sql(sql_text):
    global seq_id
    req = urllib.request.Request(
        f"{sf_host}/queries/v1/query-request?requestId={uuid.uuid4()}",
        data=json.dumps({"sqlText": sql_text, "sequenceId": seq_id}).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/snowflake", "Authorization": f'Snowflake Token="{sf_token}"'}
    )
    seq_id += 1
    with urllib.request.urlopen(req, timeout=60) as qresp:
        raw = gzip.decompress(qresp.read()) if qresp.headers.get("Content-Encoding") == "gzip" else qresp.read()
        payload = json.loads(raw.decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"Snowflake SQL failed: {payload.get('message')}")
        data = payload.get("data", {})
        cols = [c["name"] for c in data.get("rowtype", [])]
        return [dict(zip(cols, r)) for r in data.get("rowset", [])]

# 2. Discover Mirrored Tables in OneLake & Extract Snowflake Metadata / Relationships
_, _, tbl_status = fabric_request(
    "POST",
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/mirroredDatabases/{MIRRORED_DB_ID}/getTablesMirroringStatus"
)
mirrored_tables = {
    (t["sourceSchemaName"], t["sourceTableName"])
    for t in tbl_status.get("data", [])
    if t.get("status") in ("Replicating", "Snapshotting", "Initialized")
}

def parse_comment_and_synonyms(raw_comment):
    if not raw_comment:
        return "", []
    m = re.search(r"\[SYNONYMS:\s*([^\]]+)\]", raw_comment, flags=re.IGNORECASE)
    synonyms = [s.strip() for s in m.group(1).split(",") if s.strip()] if m else []
    clean_desc = re.sub(r"\s*\[SYNONYMS:[^\]]+\]", "", raw_comment, flags=re.IGNORECASE).strip()
    return clean_desc, synonyms

table_meta = {}
for r in run_sf_sql(f"SHOW TABLES IN SCHEMA {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}"):
    if (r["schema_name"], r["name"]) in mirrored_tables:
        desc, syns = parse_comment_and_synonyms(r.get("comment"))
        table_meta[r["name"]] = {"schema": r["schema_name"], "description": desc, "synonyms": syns, "columns": [], "pk_columns": []}

for c in run_sf_sql(f"SHOW COLUMNS IN SCHEMA {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}"):
    if c["table_name"] in table_meta:
        desc, syns = parse_comment_and_synonyms(c.get("comment"))
        dt_obj = json.loads(c["data_type"])
        table_meta[c["table_name"]]["columns"].append({
            "name": c["column_name"], "sf_type": dt_obj.get("type", "TEXT"),
            "scale": int(dt_obj.get("scale") or 0), "description": desc, "synonyms": syns
        })

for r in sorted(run_sf_sql(f"SHOW PRIMARY KEYS IN SCHEMA {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}"), key=lambda x: int(x["key_sequence"])):
    if (r["schema_name"], r["table_name"]) in mirrored_tables:
        table_meta[r["table_name"]]["pk_columns"].append(r["column_name"])

fk_groups = defaultdict(list)
for r in run_sf_sql(f"SHOW IMPORTED KEYS IN SCHEMA {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}"):
    fk_groups[r["fk_name"]].append(r)

valid_relationships = []
for fk_name, rows in fk_groups.items():
    rows_sorted = sorted(rows, key=lambda x: int(x["key_sequence"]))
    fk_s, fk_t = rows_sorted[0]["fk_schema_name"], rows_sorted[0]["fk_table_name"]
    pk_s, pk_t = rows_sorted[0]["pk_schema_name"], rows_sorted[0]["pk_table_name"]
    if (fk_s, fk_t) not in mirrored_tables or (pk_s, pk_t) not in mirrored_tables:
        continue
    col_pairs = [(r["fk_column_name"], r["pk_column_name"]) for r in rows_sorted]
    if len(col_pairs) == 1:
        valid_relationships.append({"name": fk_name, "type": "single_column", "from_table": fk_t, "from_column": col_pairs[0][0], "to_table": pk_t, "to_column": col_pairs[0][1], "pairs": col_pairs})
    else:
        fk_cols = {c["name"] for c in table_meta[fk_t]["columns"]}
        pk_cols = {c["name"] for c in table_meta[pk_t]["columns"]}
        ck_col = next(iter(sorted(c for c in (fk_cols & pk_cols) if c.startswith("CK_"))), None)
        valid_relationships.append({"name": fk_name, "type": "composite_key", "from_table": fk_t, "from_column": ck_col, "to_table": pk_t, "to_column": ck_col, "pairs": col_pairs})

print(f"Discovered {len(table_meta)} OneLake tables and {len(valid_relationships)} validated relationships.")
```

---

## 6. Troubleshooting Matrix & Cost-Control Checklist

| Symptom / Error | Root Cause | Resolution |
|---|---|---|
| **In GCP Cloud Storage $\rightarrow$ Settings $\rightarrow$ Interoperability, `Create a key for a service account` is disabled/greyed out** | GCP Organization Policy `iam.disableServiceAccountKeyCreation` is enforced by default on new GCP organizations. | Grant `roles/orgpolicy.policyAdmin` at the Organization node and override `iam.disableServiceAccountKeyCreation` to **Off** on your project (Step 6). |
| **Snowflake `SYSTEM$VERIFY_EXTERNAL_VOLUME` fails with `403 Forbidden` on GCS** | The Snowflake-generated GCP service account (`STORAGE_GCP_SERVICE_ACCOUNT`) has not been granted permissions on `<gcs-iceberg-bucket>`. | Grant **`Storage Admin` (`roles/storage.admin`)** to `<snowflake-gcp-service-account>` on the GCS bucket (Step 3). |
| **Snowflake `CREATE ICEBERG TABLE` with `FOREIGN KEY` fails with `Primary key and foreign key data type does not match`** | In Snowflake Managed tables, `INT` defaults to `NUMBER(38,0)` (128-bit), whereas in Snowflake Iceberg tables `INT` is a 32-bit integer (`NUMBER(10,0)`). | Define integer primary keys on Managed dimension tables as `NUMBER(10,0)` when referencing them from Iceberg foreign keys. |
| **Direct Lake Semantic Model framing refresh returns `ModelRefresh_ShortMessage_ProcessingError`** | The Direct Lake `AzureStorage.DataLake` expression points to a table shortcut across different items or a table whose initial snapshot has not finished yet. | Point the Direct Lake expression directly to `https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<mirroredDbId>` after `getTablesMirroringStatus` reports `Replicating`. |

---

## 7. Official Public Documentation References

1. **Microsoft Fabric — Create a Google Cloud Storage (GCS) Shortcut (HMAC Key & Endpoints):**  
   https://learn.microsoft.com/en-us/fabric/onelake/create-gcs-shortcut
2. **Microsoft Fabric — Create OneLake Shortcuts to Network-Restricted GCS/S3 via On-Premises Data Gateway:**  
   https://learn.microsoft.com/en-us/fabric/onelake/create-on-premises-shortcut
3. **Microsoft Fabric — Tutorial: Configure Mirrored Databases from Snowflake:**  
   https://learn.microsoft.com/en-us/fabric/mirroring/snowflake-tutorial
4. **Microsoft Fabric — Use Apache Iceberg Tables in OneLake:**  
   https://learn.microsoft.com/en-us/fabric/onelake/onelake-iceberg-tables
5. **Snowflake — Configure an External Volume for Google Cloud Storage:**  
   https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-gcs
