# Comprehensive End-to-End Architecture & Step-by-Step Implementation Runbook: Mirroring Firewalled AWS Snowflake & Apache Iceberg Tables into Microsoft Fabric

**Document Type:** Technical Implementation Runbook & Architecture Guide  
**Scenario:** Replicating Managed Tables & Apache Iceberg Tables from a Private/Firewalled Snowflake Environment on AWS (`<aws-region>`, e.g., `us-east-2`) into Microsoft Fabric using an On-Premises Data Gateway (OPDG).  
**Security Note:** All actual account IDs, hostnames, bucket names, IP addresses, GUIDs, and credentials in this document are masked with standardized placeholders (`<...>`).

> **Important Disclaimer — AWS Free Plan ("Starter") Sandbox Accounts vs. Standard Enterprise AWS Accounts:**
> * **Standard Enterprise AWS Accounts:** Standard/paid AWS accounts support cross-account `sts:AssumeRole` out of the box—every step in this playbook (including Snowflake `EXTERNAL VOLUME` and `CREATE ICEBERG TABLE`) runs without restriction.
> * **AWS Free Plan Starter Accounts ($100 Credit Tier):** When you create a brand-new AWS Free Plan starter account without upgrading to a standard pay-as-you-go plan, AWS places the account inside an AWS-managed Organization with a Service Control Policy (SCP) that **blocks external AWS accounts (such as Snowflake's AWS service account) from calling `sts:AssumeRole`** on IAM roles in your account (`is not authorized to perform: sts:AssumeRole`).
> * **What works without upgrading a Free Plan account:**
>   1. **Firewalled Snowflake Managed Table Mirroring via EC2 OPDG** (`ACCOUNTS_MANAGED`, `ORDERS_MANAGED`) works 100%.
>   2. **Firewalled Amazon S3 OneLake Shortcuts via EC2 OPDG & S3 Gateway VPC Endpoint** work 100% using IAM User Access Keys (`<aws-access-key-id>` / `<aws-secret-access-key>`), because IAM User keys do not use cross-account `sts:AssumeRole`.
>   3. **Snowflake External Stages (`CREATE STAGE ... CREDENTIALS=(AWS_KEY_ID=...)`)** and `COPY INTO 's3://...'` Parquet unloads also work 100% with IAM User Access Keys.
> * **What requires upgrading a Free Plan account (or using a standard AWS account):** Because Snowflake's `CREATE EXTERNAL VOLUME` syntax for S3 only accepts `STORAGE_AWS_ROLE_ARN` (IAM Role assumption) and does not accept IAM Access Keys, executing `CREATE ICEBERG TABLE ... EXTERNAL_VOLUME = '<EXTERNAL_VOLUME_NAME>'` requires either completing the 3-step **Upgrade plan** wizard in AWS Billing (which removes the Free Plan SCP while keeping your remaining credit) or using a standard enterprise AWS account.

---

## Table of Contents

1. [Executive Summary & Direct Answers to the 5 Key Architecture Questions](#1-executive-summary--direct-answers-to-the-5-key-architecture-questions)
2. [Simplified Architecture: Snowflake on AWS to Microsoft Fabric](#2-simplified-architecture-snowflake-on-aws-to-microsoft-fabric)
   - [2.1 Architecture at a Glance](#21-architecture-at-a-glance)
   - [2.2 How the Two Read Paths Work](#22-how-the-two-read-paths-work)
   - [2.3 POC Connectivity vs. Private Connectivity](#23-poc-connectivity-vs-private-connectivity)
3. [Placeholder & Parameter Reference Table (Masked Inventory)](#3-placeholder--parameter-reference-table-masked-inventory)
4. [Complete Step-by-Step Setup Instructions (Every Click, CLI Command, SQL & API Call)](#4-complete-step-by-step-setup-instructions-every-click-cli-command-sql--api-call)
   - [Step 1: Verify Snowflake Account Region, Edition & Inline Unload Parameter](#step-1-verify-snowflake-account-region-edition--inline-unload-parameter)
   - [Step 2: Create the Amazon S3 Bucket for Iceberg Storage](#step-2-create-the-amazon-s3-bucket-for-iceberg-storage)
   - [Step 3: Create the AWS IAM Role & S3 Permissions Policy for Snowflake](#step-3-create-the-aws-iam-role--s3-permissions-policy-for-snowflake)
   - [Step 4: Create the Snowflake External Volume & Bind the AWS IAM Trust Relationship](#step-4-create-the-snowflake-external-volume--bind-the-aws-iam-trust-relationship)
   - [Step 5: Create the Snowflake Database, Schema, Managed Tables, Iceberg Tables & Privileges](#step-5-create-the-snowflake-database-schema-managed-tables-iceberg-tables--privileges)
   - [Step 6: Create the AWS IAM User (or Entra OIDC Role) for Microsoft Fabric S3 Access](#step-6-create-the-aws-iam-user-or-entra-oidc-role-for-microsoft-fabric-s3-access)
   - [Step 7: Configure AWS VPC Private Connectivity (Snowflake PrivateLink & S3 VPC Endpoint)](#step-7-configure-aws-vpc-private-connectivity-snowflake-privatelink--s3-vpc-endpoint)
   - [Step 8: Provision the Windows Server EC2 Instance for the On-Premises Data Gateway](#step-8-provision-the-windows-server-ec2-instance-for-the-on-premises-data-gateway)
   - [Step 9: Install, Configure & Register the Microsoft On-Premises Data Gateway (OPDG)](#step-9-install-configure--register-the-microsoft-on-premises-data-gateway-opdg)
   - [Step 10: Verify Line-of-Sight Connectivity from the Gateway VM to Snowflake & S3](#step-10-verify-line-of-sight-connectivity-from-the-gateway-vm-to-snowflake--s3)
   - [Step 11: Lock Down the S3 Bucket Firewall & Snowflake Network Policy](#step-11-lock-down-the-s3-bucket-firewall--snowflake-network-policy)
   - [Step 12: Create Both Gateway-Routed Connections in Microsoft Fabric](#step-12-create-both-gateway-routed-connections-in-microsoft-fabric)
   - [Step 13: Pre-Validate S3 Connectivity with a Standalone OneLake S3 Shortcut](#step-13-pre-validate-s3-connectivity-with-a-standalone-onelake-s3-shortcut)
   - [Step 14: Create & Start the Mirrored Snowflake Database in Microsoft Fabric (Portal & REST API)](#step-14-create--start-the-mirrored-snowflake-database-in-microsoft-fabric-portal--rest-api)
   - [Step 15: Verify Replication Status, Query via T-SQL, & Enable OneLake Shortcut Caching](#step-15-verify-replication-status-query-via-t-sql--enable-onelake-shortcut-caching)
   - [Step 16: Automate Direct Lake Semantic Model Creation (OneLake-Scoped Tables, Single & Composite Key Relationships, Descriptions & Synonyms)](#step-16-automate-direct-lake-semantic-model-creation-onelake-scoped-tables-single--composite-key-relationships-descriptions--synonyms)
5. [End-to-End Automation Scripts (Python `boto3`, Snowflake SQL, PowerShell, Fabric REST API)](#5-end-to-end-automation-scripts-python-boto3-snowflake-sql-powershell-fabric-rest-api)
6. [Troubleshooting Matrix & Teardown / Cost-Control Script](#6-troubleshooting-matrix--teardown--cost-control-script)
7. [Official Public Documentation References](#7-official-public-documentation-references)

---

## 1. Executive Summary & Direct Answers to the 5 Key Architecture Questions

| # | Architecture Question | Complete Technical Answer |
|---|---|---|
| **1** | **Does the On-Premises Data Gateway (OPDG) need network connectivity only to the Snowflake endpoint, or must it also have direct access to the underlying S3 storage that hosts the Iceberg table data?** | **It must have network connectivity to BOTH the Snowflake endpoint AND the underlying Amazon S3 bucket.**<br/>When mirroring Snowflake Iceberg tables, Microsoft Fabric requires **two separate connections**:<br/>1. **Snowflake Connection:** Used by the Fabric Mirroring replicator engine to discover schemas/tables (`SHOW TABLES`, `DESCRIBE TABLE`), track changes via Snowflake Streams (`CREATE STREAM`), and query `SYSTEM$GET_ICEBERG_TABLE_INFORMATION` to locate the latest Iceberg metadata file pointer.<br/>2. **Amazon S3 Storage Connection:** Used by OneLake Shortcuts under the hood to read the Iceberg metadata (`.metadata.json`, manifest lists, manifests) and `.parquet` data files **directly from S3** without routing the file reads through Snowflake's SQL engine.<br/>Selecting an OPDG on the Snowflake connection does **not** automatically proxy S3 traffic through Snowflake—both connections must be bound to the OPDG, and the OPDG host must resolve and route to both endpoints. |
| **2** | **Since Fabric reads Iceberg data directly from the underlying storage rather than through Snowflake, is a separate private route required between the gateway and the S3 bucket?** | **Yes—a separate private network route/endpoint to S3 is required, though NOT a separate physical circuit or VPN.**<br/>• A Snowflake AWS PrivateLink endpoint (`*.privatelink.snowflakecomputing.com`) only terminates inside Snowflake's service VPC; it cannot route or proxy traffic to your S3 bucket.<br/>• **If the OPDG is hosted on a Windows EC2 VM inside your AWS VPC (Recommended):** You attach a free **S3 Gateway VPC Endpoint** (`com.amazonaws.<region>.s3`) or **S3 Interface VPC Endpoint** to the EC2 subnet's route table. The EC2 gateway VM reaches Snowflake via the Snowflake PrivateLink endpoint and reaches S3 via the S3 VPC endpoint—both 100% privately inside the AWS VPC.<br/>• **If the OPDG is hosted on-premises or in Azure over VPN / Direct Connect:** AWS **does not** allow S3 Gateway Endpoints to be accessed across VPN or Direct Connect. You **must** provision an **S3 Interface VPC Endpoint (AWS PrivateLink for S3)** with private IPs and conditional DNS forwarding. |
| **3** | **Are there any specific network, firewall, private endpoint, or gateway configuration requirements that must be met when mirroring Snowflake Iceberg tables stored in AWS S3?** | **Yes, across three layers:**<br/>**A. Snowflake Layer:**<br/>• Configure AWS PrivateLink (`SYSTEM$AUTHORIZE_PRIVATELINK`) + Private DNS (`*.privatelink.snowflakecomputing.com` & OCSP endpoints), or allow the Gateway's IP in a Snowflake Network Policy.<br/>• Ensure the Snowflake account parameter `PREVENT_UNLOAD_TO_INLINE_URL = FALSE` (default). If set to `TRUE`, gateway-based mirroring is completely blocked.<br/>• Grant the Snowflake mirroring user `CREATE STREAM`, `SELECT`, `SHOW TABLES`, and `DESCRIBE TABLES` privileges.<br/>**B. Amazon S3 Layer:**<br/>• Keep **S3 Block Public Access = ON** (Microsoft explicitly supports S3 shortcuts with Block Public Access enabled).<br/>• Grant `s3:GetObject`, `s3:ListBucket`, and `s3:GetBucketLocation` (plus `kms:Decrypt` / `kms:GenerateDataKey` if using SSE-KMS Bucket Keys).<br/>• Configure the S3 Bucket Policy to allow requests from your S3 VPC Endpoint (`aws:SourceVpce: <s3-vpc-endpoint-id>`).<br/>**C. On-Premises Data Gateway (OPDG) Layer:**<br/>• Install the latest **Standard Mode** OPDG (not Personal Mode) on a supported 64-bit Windows Server (recommended: 8 vCPU, 16 GB RAM for production; minimum 4+ GB RAM/pagefile for POC).<br/>• **No inbound ports** are needed on the gateway firewall.<br/>• Allow **outbound TCP 443** (and optionally TCP 5671–5672, 9350–9354, or enable `Force HTTPS Mode` in the OPDG app so only TCP 443 is used) to `*.servicebus.windows.net`, `*.powerbi.com`, `*.analysis.windows.net`, and `login.microsoftonline.com`. |
| **4** | **Can you share recommended architecture diagrams, deployment guidance, or best practices for implementing this scenario securely over a private connection?** | See **Section 2** for the simplified AWS-only architecture and **Section 4** for the 15-step deployment runbook.<br/>**Top Best Practice:** Deploy the OPDG on a **Windows Server EC2 instance inside your AWS VPC, in the same Region** as your Snowflake account and S3 bucket. With direct AWS outbound routing to Microsoft services, this avoids sending data through your corporate WAN and lets you use an S3 Gateway VPC Endpoint. Snowflake itself runs in Snowflake's service environment, not in your AWS VPC. |
| **5** | **Are there any known limitations or additional prerequisites we should consider before proceeding with the proof of concept?** | 1. **`PREVENT_UNLOAD_TO_INLINE_URL = TRUE` blocks OPDG/VNet Gateways:** Verify this is `FALSE` in Snowflake.<br/>2. **One storage connection per Mirrored Database:** All Iceberg tables selected in a single Mirrored Snowflake item must be reachable through the same S3 storage connection.<br/>3. **OPDG vs. VNet Data Gateway:** While Snowflake connections support both VNet Data Gateways and OPDG, **OneLake S3 Shortcuts to firewalled S3 buckets only support OPDG** (not VNet Data Gateways). Therefore, OPDG is mandatory when S3 is firewalled.<br/>4. **Workspace Identity not supported for Snowflake:** Use Snowflake Basic (`Username/Password`), `KeyPair`, or `Entra ID SSO` for Snowflake, and `IAM Access Key` or `Entra OIDC Service Principal` for S3.<br/>5. **Case Sensitivity:** All Snowflake identifiers (Warehouse, Database, Schema, Table) are strictly case-sensitive in Fabric.<br/>6. **1,000-Table Limit & No Paid Capacity Trial Lockouts:** Up to 1,000 tables per mirrored database; deploy in a regular Fabric workspace (not `My Workspace`) backed by an active Fabric F-SKU or Trial capacity.<br/>7. **Enable OneLake S3 Shortcut Caching:** Turn on file caching (1–28 days TTL) to reduce repeated AWS S3 egress charges for cached files (`<= 1 GB` per file; changed files and first-time reads still egress from S3).<br/>8. **Iceberg Format & Type Compatibility:** Source Iceberg tables must have `< 5,000` commits, use Parquet data files, and contain only one metadata file set per table folder. Avoid `DECIMAL` with precision `>= 10` (or apply the documented Spark/SQL analytics endpoint workaround), `TIMESTAMP_NTZ` (prefer `TIMESTAMP_LTZ`), and equality deletes.<br/>9. **Operational Lifecycle & Security:** Snowflake RLS/CLS does not replicate automatically (reapply in Fabric). DDL changes or stopping/restarting mirroring trigger a full table reseed, and idle polling backs off up to 1 hour. |

---

## 2. Simplified Architecture: Snowflake on AWS to Microsoft Fabric

**Scope:** Snowflake hosted on AWS, Iceberg files in Amazon S3, and a Windows EC2 instance running the On-Premises Data Gateway (OPDG). No Snowflake-on-Azure or Snowflake-on-GCP architectures are included.

The diagram shows the proposed private-source design, not a verification of the live POC. Resource names remain masked. The component guide below explains the architecture in detail; executable policies, DNS configuration, and installation commands remain in the setup steps.

### 2.1 Architecture at a Glance

**One design, three areas:** AWS source services -> gateway in the AWS VPC -> Microsoft Fabric. The numbered boxes map to the component guide below. Two separate source-read paths share one gateway; there is no need to duplicate the EC2 gateway for each connection.

![Snowflake on AWS and S3 to Microsoft Fabric through an EC2 gateway](https://raw.githubusercontent.com/kollasriram/snowflake-aws-iceberg-fabric-mirroring/main/Snowflake_AWS_Iceberg_Fabric_Architecture.png)


**Read the diagram left to right:** Snowflake owns the table definition, S3 holds the files, two AWS endpoints give the EC2 gateway private access to those sources, and Fabric uses two connections to expose the table for analytics.

**Arrow legend:** **A** is Snowflake's storage access. **B** and **C** are the two independent Fabric read paths. **D** is their shared gateway-to-Microsoft communication channel. Arrows describe logical data flow, not the direction in which network sessions are opened. The gateway initiates its source requests and its outbound Microsoft connection; Fabric does not require an inbound connection to the EC2 host.

#### Component guide: what each box contains

| Box | Contents and purpose | Important configuration |
|---|---|---|
| **1. Snowflake on AWS** | A warehouse, database, schema, Snowflake-managed Iceberg table, external volume, and mirroring service user/role. Snowflake supplies table discovery and the current Iceberg metadata location. | Use the correct identifier casing, required table/catalog permissions, and a supported authentication method. Configure the external volume's S3 location and IAM trust. Snowflake runs in its service environment, not in the AWS VPC. |
| **2. Amazon S3** | Bucket `<s3-iceberg-bucket>` with the Iceberg table's metadata JSON, manifest/manifest-list Avro files, and Parquet data files under its configured prefix. | Keep Block Public Access enabled. Scope IAM access to the required bucket/prefix. Preserve Snowflake read/write access and Fabric read access separately. If using SSE-KMS, authorize the required principals in both IAM and the KMS key policy. |
| **3. Snowflake endpoint** | An AWS PrivateLink interface endpoint in the AWS VPC provides a private destination for gateway-to-Snowflake traffic. | Confirm Snowflake edition/account eligibility, endpoint authorization, private hostname/DNS resolution, endpoint security group, and Snowflake network rules. Include any additional Snowflake client endpoints required by its current private-connectivity guidance. |
| **4. S3 endpoint** | An S3 Gateway VPC Endpoint routes the EC2 host's regional S3 requests through AWS private routing. | Associate the route table used by the gateway subnet. Check endpoint policy, bucket policy, IAM, DNS, and regional alignment. This endpoint is not an EC2 network interface and does not have an interface-endpoint security group. |
| **5. EC2 gateway** | Windows EC2 instance `<opdg-ec2-instance-id>` runs the standard-mode OPDG service and belongs to registered cluster `<opdg-cluster-name>`. It connects to both source services and Microsoft gateway services. | Allow required DNS resolution and outbound source/Microsoft connectivity. Use gateway HTTPS mode for the illustrated HTTPS 443 channel; this is not a complete firewall allowlist. Plan memory, disk/pagefile, patching, monitoring, and restricted administrative access. No inbound rule is needed for OPDG runtime traffic. |
| **6. Fabric connections** | Connection `<fabric-snowflake-connection-name>` provides Snowflake access. Connection `<fabric-s3-connection-name>` provides independent storage access. Both select the registered gateway cluster in this design. | Configure each connection's endpoint, supported authentication, credential scope, gateway selection, and user permissions independently. Do not assume successful Snowflake authentication proves S3 access. Confirm that the chosen S3 connection supports gateway routing in the target environment. |
| **7. Mirrored database** | Item `<fabric-mirrored-db-name>` selects the Snowflake database/tables and associates the Iceberg storage connection. | Use an active Fabric capacity and appropriate workspace/item permissions. All selected Iceberg tables must be reachable through the same storage connection; split mirrored database items when that is not possible. Monitor table-level replication, not just item creation. |
| **8. OneLake** | Mirroring maintains Iceberg metadata and uses shortcuts to the S3 files, with Delta-format interoperability for Fabric engines. | This is not the same as copying all Iceberg data into OneLake. Uncached file reads still depend on S3, the gateway, and source permissions. Optional shortcut caching is a separate setting, not a prerequisite or a guarantee of zero AWS egress. |
| **9. Analytics** | The SQL analytics endpoint, Power BI, and Spark provide downstream access to the mirrored tables as supported by each workload. | Validate table visibility and queries before building reports. Reapply required security in Fabric; do not assume Snowflake grants or source row/column restrictions automatically transfer. |

#### Credentials and security boundaries

- **Snowflake-to-S3 identity:** `<snowflake-iceberg-iam-role>` is assumed through the external volume's configured trust relationship and external ID. It manages the Iceberg files.
- **Fabric-to-Snowflake identity:** `<snowflake-mirror-user>` and its Snowflake role authorize catalog/table access through the Snowflake connection.
- **Fabric-to-S3 identity:** The storage connection uses separate supported credentials with read access to the required S3 prefix. The Windows instance profile is not automatically the storage connection's identity.
- **Gateway registration:** The gateway administrator registers the cluster with the appropriate Microsoft tenant and protects its recovery key. Gateway registration, connection access, and workspace/item access are separate permission checks.
- **Network versus identity:** Private endpoints control the network route; they do not grant table or object permissions. IAM/Snowflake permissions do not, by themselves, make a route private.
- **Bucket policy caution:** Do not apply a blanket deny for every request outside the gateway's S3 endpoint without accounting for Snowflake's own storage path. An explicit deny can break Iceberg writes even when the IAM role otherwise allows them.

#### Runtime flow in six steps

1. **Snowflake publishes a table snapshot.** The external volume and its IAM role let Snowflake maintain Iceberg metadata and data files in S3.
2. **Fabric requests catalog information.** Through the Snowflake connection and gateway, it discovers the selected tables and obtains their Iceberg metadata locations over the Snowflake private path.
3. **Fabric accesses storage independently.** Through the S3 connection and gateway, it reads the referenced metadata, manifests, and required Parquet files using the S3 endpoint route. Snowflake's SQL engine does not proxy these file reads.
4. **The gateway communicates outward.** Responses travel over the gateway's outbound encrypted Microsoft channel. The relay and supporting Microsoft service endpoints are collapsed into arrow **D** to keep the diagram readable.
5. **OneLake exposes the table.** Mirrored metadata and shortcuts make the Iceberg table available to Fabric engines with Delta-format interoperability.
6. **Consumers query and changes become visible.** SQL, Power BI, or Spark reads use the available table metadata and storage path. Measure freshness with a controlled source change and downstream readback; do not assume immediate visibility.

These steps explain the logical flow, not a claim that each analytics query makes a new Snowflake call or that each refresh copies every Parquet file.

#### What must be proven in the lab

| Check | Evidence to capture |
|---|---|
| **Private Snowflake route** | DNS resolution from the EC2 host, successful Snowflake connection, and endpoint/network-policy evidence showing that the intended PrivateLink path is used. A successful TCP test alone is insufficient. |
| **Private S3 route** | Correct subnet route-table association and endpoint policy, plus an authenticated read of the table's actual metadata/data objects through the selected route. |
| **Both gateway connections** | Gateway cluster online; Snowflake connection test succeeds; S3 connection test and an actual object read succeed independently. |
| **Fabric queryability** | Selected Iceberg table reaches the expected replication state and a query returns known source records. Save the replication status and query result. |
| **Change propagation** | Insert or update a known test record in Snowflake, record the source timestamp, and read it back in Fabric with the observed visibility time. |
| **Security and dependency behavior** | During an approved test window, verify the intended access restrictions without locking out administrators or breaking Snowflake storage access. Record gateway/source dependencies and recovery steps. |

**Not shown as extra boxes:** IAM roles, KMS, DNS, security groups, Microsoft relay services, EC2 management, and optional caching. They remain essential supporting configuration, but drawing every dependency would obscure the two main read paths. Production may also require a multi-node gateway cluster and resilience testing; the single EC2 host shown is the POC topology, not a highly available deployment.

### 2.2 How the Two Read Paths Work

| Path | What it does | Where it is configured |
|---|---|---|
| **1. Snowflake catalog** | Fabric uses the Snowflake connection through the gateway to discover tables and obtain Iceberg metadata locations. | Snowflake connection, gateway selection, and Snowflake endpoint/DNS configuration. |
| **2. S3 files** | Fabric's storage access uses the separate S3 connection through the gateway to read Iceberg metadata and Parquet files. File reads do not pass through Snowflake's SQL engine. | Amazon S3 connection, gateway selection, S3 IAM permissions, endpoint routing, and bucket policy. |
| **3. AWS to Fabric** | The EC2-hosted gateway communicates with Microsoft services over an outbound encrypted connection. | Gateway network settings and AWS outbound firewall/routing rules. |

**Two Fabric connections, one gateway host:** Both connections use the same OPDG cluster in this design, but they have separate credentials and network destinations.

Snowflake's external-volume IAM role authorizes Snowflake to manage the S3 files. Fabric's separate S3 identity authorizes reads. Bucket policies must preserve access for both identities; Snowflake's own storage traffic does not automatically traverse the gateway's S3 endpoint.

The diagram focuses on Iceberg tables. The optional regular Snowflake tables in the lab use row replication rather than the Iceberg shortcut file-read path.

### 2.3 POC Connectivity vs. Private Connectivity

| Segment | IP-restricted POC option | Private-source option shown above |
|---|---|---|
| **Gateway to Snowflake** | Public Snowflake HTTPS endpoint, restricted to the gateway's stable outbound public IP. | Snowflake AWS PrivateLink endpoint with private DNS and appropriate Snowflake network rules. |
| **Gateway to S3** | Authenticated S3 HTTPS access with appropriate IAM and bucket policies. S3 Block Public Access stays enabled. | S3 Gateway VPC Endpoint associated with the EC2 subnet's route table, with appropriate IAM and bucket policies. |
| **Gateway to Fabric** | Outbound encrypted connection to Microsoft service endpoints. | Same outbound connection; AWS PrivateLink for Snowflake does not make this cross-cloud segment private. |

**Important distinctions:**

- An IP-allowlisted public Snowflake endpoint is **not** AWS PrivateLink.
- S3 Block Public Access prevents public grants; it does **not** by itself require private endpoint access.
- This design needs no corporate VPN, ExpressRoute, or Direct Connect when the EC2 gateway has direct outbound access to Microsoft services. Corporate network load depends on routing; avoid corporate proxies or forced corporate transit if that is the goal.
- Hosting the gateway on EC2 avoids a corporate-network dependency, but the EC2 instance, AWS network, and outbound link still carry the traffic.
- An entirely private AWS-to-Microsoft transport is **not established by this diagram** and requires separate design and support validation.

**Setup order:** S3 and IAM trust -> Snowflake external volume and tables -> AWS endpoints and EC2 gateway -> two Fabric connections -> mirrored database -> query validation. See Section 4 for the detailed steps and scripts.

**Microsoft references:** [Snowflake mirroring overview](https://learn.microsoft.com/en-us/fabric/mirroring/snowflake) and [Snowflake mirroring setup tutorial](https://learn.microsoft.com/en-us/fabric/mirroring/snowflake-tutorial) describe Iceberg storage connections and gateway access to firewalled Snowflake. Verify the S3 connection's gateway selection and successful file reads independently; a working Snowflake connection alone does not validate the S3 path.

---

## 3. Placeholder & Parameter Reference Table (Masked Inventory)

When executing the scripts in **Section 4** and **Section 5**, replace these masked placeholders with your environment's values:

| Masked Placeholder | Example Format / Value | Description |
|---|---|---|
| `<aws-account-id>` | `123456789012` | 12-digit AWS Account ID hosting the S3 bucket and VPC |
| `<aws-region>` | `us-east-2` | AWS Region matching your Snowflake account region |
| `<s3-iceberg-bucket>` | `company-fabric-snowflake-iceberg-poc` | S3 bucket name storing Iceberg metadata and Parquet files |
| `<aws-vpc-id>` | `vpc-0abc123def4567890` | AWS VPC ID hosting the private endpoints and EC2 gateway VM |
| `<vpc-route-table-id>` | `rtb-0abc123def4567890` | Route table ID associated with the EC2 gateway subnet |
| `<s3-vpc-endpoint-id>` | `vpce-0abc123def4567890` | S3 Gateway (or Interface) VPC Endpoint ID in `<aws-region>` |
| `<opdg-ec2-instance-id>` | `i-0abc123def4567890` | Windows Server 2022 EC2 Instance ID running the OPDG |
| `<opdg-gateway-ip>` | `203.0.113.50` | Public/Elastic IP (or Private IP over PrivateLink) of the EC2 Gateway VM |
| `<admin-workstation-ip>` | `198.51.100.25` | Your admin workstation IP for initial setup/management |
| `<snowflake-iceberg-iam-role>` | `SnowflakeIcebergRole` | AWS IAM Role assumed by Snowflake's External Volume |
| `<fabric-s3-iam-user>` | `fabric-s3-shortcut-user` | AWS IAM User (or OIDC Role) used by Fabric's S3 connection |
| `<aws-access-key-id>` | `AKIAXXXXXXXXXXXXXXXX` | Access Key ID for `<fabric-s3-iam-user>` |
| `<aws-secret-access-key>` | `****************************************` | Secret Access Key for `<fabric-s3-iam-user>` |
| `<snowflake-account>` | `orgname-acctname` | Snowflake account identifier (`<snowflake-account>.snowflakecomputing.com`) |
| `<snowflake-mirror-user>` | `SVC_FABRIC_MIRROR_USER` | Snowflake user configured in the Fabric Snowflake connection |
| `<snowflake-mirror-password>` | `********************` | Password (or RSA Private Key) for `<snowflake-mirror-user>` |
| `<SNOWFLAKE_WAREHOUSE>` | `COMPUTE_WH` | Uppercase/exact-case Snowflake virtual warehouse name |
| `<SNOWFLAKE_DB>` | `FABRIC_ICEBERG_POC_DB` | Uppercase/exact-case Snowflake database name |
| `<SNOWFLAKE_SCHEMA>` | `POC_SCHEMA` | Uppercase/exact-case Snowflake schema name |
| `<EXTERNAL_VOLUME_NAME>` | `EXVOL_FABRIC_ICEBERG_S3` | Snowflake External Volume object name |
| `<snowflake-aws-iam-user-arn>` | `arn:aws:iam::111122223333:user/xxxx-s` | Returned by `DESC EXTERNAL VOLUME` in Snowflake |
| `<snowflake-aws-external-id>` | `ABC12345_SFCRole=5_xxxxxxxxxxxx=` | Returned by `DESC EXTERNAL VOLUME` in Snowflake |
| `<opdg-cluster-name>` | `AWS-OPDG-CLUSTER-POC` | Name of the On-Premises Data Gateway cluster registered in Fabric |
| `<opdg-gateway-cluster-id>` | `00000000-0000-0000-0000-000000000001` | GUID of the registered OPDG cluster in Fabric |
| `<fabric-snowflake-connection-id>` | `00000000-0000-0000-0000-000000000002` | GUID of the gateway-routed Snowflake connection in Fabric |
| `<fabric-s3-connection-id>` | `00000000-0000-0000-0000-000000000003` | GUID of the gateway-routed Amazon S3 connection in Fabric |
| `<fabric-workspace-id>` | `00000000-0000-0000-0000-000000000004` | GUID of the target Microsoft Fabric workspace |
| `<fabric-lakehouse-id>` | `00000000-0000-0000-0000-000000000005` | GUID of the Fabric Lakehouse used for standalone S3 shortcut testing |
| `<fabric-mirrored-db-id>` | `00000000-0000-0000-0000-000000000006` | GUID of the Mirrored Snowflake Database item in Fabric |
| `<SNOWFLAKE_MIRROR_ROLE>` | `FABRIC_MIRROR_ROLE` | Snowflake role granted `USAGE`, `CREATE STREAM`, and `SELECT` privileges |
| `<snowflake-admin-user>` | `ADMIN_USER` | `ACCOUNTADMIN` Snowflake user used for initial provisioning |
| `<snowflake-admin-password>` | `********************` | Password for `<snowflake-admin-user>` |
| `<fabric-entra-bearer-token>` | `eyJ0eXAiOiJKV1QiLC...` | Entra ID token from `az account get-access-token --resource https://api.fabric.microsoft.com` |
| `<entra-tenant-id>` | `00000000-0000-0000-0000-000000000007` | Microsoft Entra ID Tenant GUID (Option 6B OIDC only) |
| `<entra-client-id>` | `00000000-0000-0000-0000-000000000008` | Microsoft Entra App Registration Client ID (Option 6B OIDC only) |
| `<entra-enterprise-sp-object-id>` | `00000000-0000-0000-0000-000000000009` | Enterprise Application Object ID used in AWS OIDC `sub` claim (Option 6B OIDC only) |
| `<fabric-s3-oidc-role>` | `FabricOneLakeS3OIDCRole` | AWS IAM Role assumed by Fabric via Entra OIDC Federation (Option 6B OIDC only) |

---

## 4. Complete Step-by-Step Setup Instructions (Every Click, CLI Command, SQL & API Call)

---

### Step 1: Verify Snowflake Account Region, Edition & Inline Unload Parameter

Before provisioning any AWS resources, confirm your Snowflake account's exact AWS region and verify that `PREVENT_UNLOAD_TO_INLINE_URL` is `false`.

#### 1.1 Check DNS CNAME / Region from PowerShell (Optional)
```powershell
Resolve-DnsName "<snowflake-account>.snowflakecomputing.com"
```

#### 1.2 Run Pre-Flight Checks in Snowflake Worksheet
Log into Snowflake as `ACCOUNTADMIN` and run:
```sql
USE ROLE ACCOUNTADMIN;

-- 1. Confirm AWS Region (e.g., AWS_US_EAST_2), Account Locator, and Version
SELECT CURRENT_REGION() AS SNOWFLAKE_REGION,
       CURRENT_ACCOUNT() AS ACCOUNT_LOCATOR,
       CURRENT_VERSION() AS SNOWFLAKE_VERSION;

-- Confirm Snowflake Edition (Business Critical is required for AWS PrivateLink;
-- Standard/Enterprise editions work with the IP-allowlisted OPDG POC pattern):
SELECT SYSTEM$SHOW_ACCOUNT_PARAMETERS(); -- Or check Admin -> Accounts in Snowsight UI

-- 2. CRITICAL: Verify PREVENT_UNLOAD_TO_INLINE_URL is FALSE
-- If TRUE, On-Premises Data Gateway and VNet Data Gateway mirroring are blocked!
SHOW PARAMETERS LIKE 'PREVENT_UNLOAD_TO_INLINE_URL' IN ACCOUNT;

-- If it is set to TRUE, set it to FALSE for the POC:
ALTER ACCOUNT SET PREVENT_UNLOAD_TO_INLINE_URL = FALSE;
```

---

### Step 2: Create the Amazon S3 Bucket for Iceberg Storage

Create the S3 bucket in the **exact same AWS region** (`<aws-region>`) as your Snowflake account to avoid cross-region data transfer charges.

#### Option A: Via AWS CLI
```bash
aws s3api create-bucket \
  --bucket <s3-iceberg-bucket> \
  --region <aws-region> \
  --create-bucket-configuration LocationConstraint=<aws-region>

# Ensure S3 Block Public Access is ON (Recommended & Supported by Fabric OneLake)
aws s3api put-public-access-block \
  --bucket <s3-iceberg-bucket> \
  --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

#### Option B: Via AWS Management Console
1. Open the **Amazon S3 Console** and switch the top-right region selector to **`<aws-region>`** (e.g., `US East (Ohio) us-east-2`).
2. Click **Create bucket**.
3. **Bucket type:** `General purpose`.
4. **Bucket name:** Enter `<s3-iceberg-bucket>`.
5. **Block Public Access settings for this bucket:** Keep **Block all public access** checked (`ON`).
6. **Default encryption:** Select `SSE-S3` (or `SSE-KMS` with Bucket Key enabled—if using `SSE-KMS`, remember to grant `kms:Decrypt` to the Fabric S3 IAM identity).
7. Click **Create bucket**.

---

### Step 3: Create the AWS IAM Role & S3 Permissions Policy for Snowflake

Snowflake requires an AWS IAM Role (`<snowflake-iceberg-iam-role>`) to write and manage Iceberg metadata and Parquet files in `s3://<s3-iceberg-bucket>/iceberg/`.

1. Create a file named `snowflake_s3_permissions.json`:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:PutObject",
           "s3:GetObject",
           "s3:GetObjectVersion",
           "s3:DeleteObject",
           "s3:DeleteObjectVersion"
         ],
         "Resource": "arn:aws:s3:::<s3-iceberg-bucket>/iceberg/*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "s3:ListBucket",
           "s3:GetBucketLocation"
         ],
         "Resource": "arn:aws:s3:::<s3-iceberg-bucket>",
         "Condition": {
           "StringLike": {
             "s3:prefix": [
               "iceberg/*"
             ]
           }
         }
       }
     ]
   }
   ```
2. Create an initial placeholder trust policy (`initial_trust.json`) so the role ARN exists before we run `CREATE EXTERNAL VOLUME` in Snowflake:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "AWS": "arn:aws:iam::<aws-account-id>:root"
         },
         "Action": "sts:AssumeRole"
       }
     ]
   }
   ```
3. Run the AWS CLI commands to create the role and attach the permissions policy:
   ```bash
   aws iam create-role \
     --role-name <snowflake-iceberg-iam-role> \
     --assume-role-policy-document file://initial_trust.json

   aws iam put-role-policy \
     --role-name <snowflake-iceberg-iam-role> \
     --policy-name <snowflake-s3-policy-name> \
     --policy-document file://snowflake_s3_permissions.json
   ```

---

### Step 4: Create the Snowflake External Volume & Bind the AWS IAM Trust Relationship

1. In Snowflake (`ACCOUNTADMIN`), create the `EXTERNAL VOLUME` pointing to your S3 bucket and IAM Role ARN:
   ```sql
   USE ROLE ACCOUNTADMIN;

   CREATE OR REPLACE EXTERNAL VOLUME <EXTERNAL_VOLUME_NAME>
     STORAGE_LOCATIONS = (
       (
         NAME = 's3_iceberg_primary'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://<s3-iceberg-bucket>/iceberg/'
         STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws-account-id>:role/<snowflake-iceberg-iam-role>'
       )
     )
     ALLOW_WRITES = TRUE;

   -- Retrieve the Snowflake-generated AWS IAM User ARN and External ID
   DESC EXTERNAL VOLUME <EXTERNAL_VOLUME_NAME>;
   ```
2. In the output of `DESC EXTERNAL VOLUME <EXTERNAL_VOLUME_NAME>`, look at the JSON inside `STORAGE_LOCATION_1` and copy:
   - `STORAGE_AWS_IAM_USER_ARN`: `<snowflake-aws-iam-user-arn>`
   - `STORAGE_AWS_EXTERNAL_ID`: `<snowflake-aws-external-id>`
3. Update the Trust Policy on `<snowflake-iceberg-iam-role>` in AWS with those two exact values:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "AllowSnowflakeExternalVolumeAssumeRole",
         "Effect": "Allow",
         "Principal": {
           "AWS": "<snowflake-aws-iam-user-arn>"
         },
         "Action": "sts:AssumeRole",
         "Condition": {
           "StringEquals": {
             "sts:ExternalId": "<snowflake-aws-external-id>"
           }
         }
       }
     ]
   }
   ```
   ```bash
   aws iam update-assume-role-policy \
     --role-name <snowflake-iceberg-iam-role> \
     --policy-document file://snowflake_trust_policy.json
   ```
4. Wait ~15 seconds for IAM propagation, then verify the External Volume handshake in Snowflake:
   ```sql
   SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('<EXTERNAL_VOLUME_NAME>');
   ```
   > **Disclaimer for AWS Free Plan ("Starter") Accounts:**  
   > If `SYSTEM$VERIFY_EXTERNAL_VOLUME` or `CREATE ICEBERG TABLE` returns `Error assuming AWS_ROLE: User: arn:aws:iam::<snowflake-service-acct>:user/... is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::<aws-account-id>:role/<snowflake-iceberg-iam-role>` even though your Trust Policy and External ID match, your AWS account is on the **AWS Free Plan starter tier**. AWS attaches a mandatory Organization SCP to Free Plan starter accounts that blocks cross-account `sts:AssumeRole` from external AWS accounts (including Snowflake).  
   > - **In a standard/enterprise AWS account:** This error does not occur.  
   > - **If you do not want to complete the 3-step AWS Billing Upgrade in a personal Free Plan lab:** You can still test and demonstrate (1) **Snowflake Managed Table Mirroring** (`ACCOUNTS_MANAGED`, `ORDERS_MANAGED`) through the EC2 OPDG, and (2) **Firewalled S3 OneLake Shortcuts** (Step 13) through the EC2 OPDG and S3 Gateway VPC Endpoint by staging Iceberg metadata/Parquet files in S3 using IAM User Access Keys (`<aws-access-key-id>`).

---

### Step 5: Create the Snowflake Database, Schema, Managed Tables, Iceberg Tables & Privileges

Run the following SQL in Snowflake to create:
1. A Managed Table (`ACCOUNTS_MANAGED`)
2. A second Managed Table (`ORDERS_MANAGED`)
3. A Snowflake-Managed Apache Iceberg Table (`ORDERS_ICEBERG`) stored in `s3://<s3-iceberg-bucket>/iceberg/orders_iceberg/`
4. The required privileges for the Snowflake user that Fabric Mirroring will use:

```sql
USE ROLE ACCOUNTADMIN;

-- 1. Create or verify the Snowflake Virtual Warehouse
CREATE WAREHOUSE IF NOT EXISTS <SNOWFLAKE_WAREHOUSE>
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE <SNOWFLAKE_WAREHOUSE>;

-- 2. Create Database and Schema
CREATE DATABASE IF NOT EXISTS <SNOWFLAKE_DB>;
CREATE SCHEMA IF NOT EXISTS <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>;

-- 3. Create Managed Table #1 (with CHANGE_TRACKING = TRUE)
CREATE OR REPLACE TABLE <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ACCOUNTS_MANAGED (
    ACCOUNT_ID INT,
    ACCOUNT_NAME STRING,
    SEGMENT STRING,
    COUNTRY STRING,
    CREATED_AT TIMESTAMP_LTZ
) CHANGE_TRACKING = TRUE;

INSERT INTO <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ACCOUNTS_MANAGED VALUES
    (101, 'Account Alpha',   'Enterprise', 'USA',     CURRENT_TIMESTAMP()),
    (102, 'Account Bravo',   'Commercial', 'Canada',  CURRENT_TIMESTAMP()),
    (103, 'Account Charlie', 'Enterprise', 'UK',      CURRENT_TIMESTAMP()),
    (104, 'Account Delta',   'MidMarket',  'USA',     CURRENT_TIMESTAMP()),
    (105, 'Account Echo',    'Enterprise', 'Germany', CURRENT_TIMESTAMP());

-- 4. Create Managed Table #2 (with CHANGE_TRACKING = TRUE)
-- Note: Using DECIMAL(9,2) (precision < 10) avoids the known Snowflake Iceberg-to-Delta
-- type-width issue on DECIMAL(>=10) columns in Fabric engines.
CREATE OR REPLACE TABLE <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ORDERS_MANAGED (
    ORDER_ID INT,
    ACCOUNT_ID INT,
    PRODUCT_NAME STRING,
    ORDER_AMOUNT DECIMAL(9,2),
    ORDER_STATUS STRING,
    ORDER_DATE DATE
) CHANGE_TRACKING = TRUE;

INSERT INTO <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ORDERS_MANAGED VALUES
    (5001, 101, 'Analytics Platform Tier 1', 12500.00, 'COMPLETED',  CURRENT_DATE()),
    (5002, 102, 'Data Integration Add-On',    4300.50, 'PROCESSING', CURRENT_DATE()),
    (5003, 103, 'Enterprise Lakehouse Pack', 98000.00, 'COMPLETED',  CURRENT_DATE()),
    (5004, 104, 'Storage Shortcut Module',    7650.25, 'COMPLETED',  CURRENT_DATE()),
    (5005, 105, 'Real-Time Streaming Tier',  15400.00, 'SHIPPED',    CURRENT_DATE());

-- 5. Create Snowflake-Managed Apache Iceberg Table in S3
-- IMPORTANT: If re-running CREATE OR REPLACE ICEBERG TABLE on an existing S3 prefix,
-- first remove any previous metadata files under s3://<s3-iceberg-bucket>/iceberg/orders_iceberg/
-- because OneLake Iceberg virtualization requires a single metadata set per table folder.
CREATE OR REPLACE ICEBERG TABLE <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ORDERS_ICEBERG (
    ORDER_ID INT,
    ACCOUNT_ID INT,
    PRODUCT_NAME STRING,
    ORDER_AMOUNT DECIMAL(9,2),
    ORDER_STATUS STRING,
    ORDER_DATE DATE
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = '<EXTERNAL_VOLUME_NAME>'
  BASE_LOCATION = 'orders_iceberg/'
  CHANGE_TRACKING = TRUE;

INSERT INTO <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ORDERS_ICEBERG
SELECT * FROM <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ORDERS_MANAGED;

-- 6. Inspect the exact Iceberg metadata file path in S3 (Used by Fabric Mirroring)
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('<SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ORDERS_ICEBERG');

-- 7. Create the Snowflake Mirroring Role & User and Assign Default Role
-- (Microsoft Fabric requires the Snowflake user to have at least one assigned role)
CREATE ROLE IF NOT EXISTS <SNOWFLAKE_MIRROR_ROLE>;

CREATE USER IF NOT EXISTS <snowflake-mirror-user>
  PASSWORD = '<snowflake-mirror-password>'
  DEFAULT_ROLE = <SNOWFLAKE_MIRROR_ROLE>
  DEFAULT_WAREHOUSE = <SNOWFLAKE_WAREHOUSE>
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE <SNOWFLAKE_MIRROR_ROLE> TO USER <snowflake-mirror-user>;

-- 8. Grant Required Mirroring Privileges to the Snowflake Mirroring Role
GRANT USAGE ON WAREHOUSE <SNOWFLAKE_WAREHOUSE> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT USAGE ON DATABASE <SNOWFLAKE_DB> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT USAGE, CREATE STREAM ON SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT SELECT ON FUTURE TABLES IN SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
GRANT USAGE ON EXTERNAL VOLUME <EXTERNAL_VOLUME_NAME> TO ROLE <SNOWFLAKE_MIRROR_ROLE>;
```

---

### Step 6: Create the AWS IAM User (or Entra OIDC Role) for Microsoft Fabric S3 Access

Microsoft Fabric's OneLake S3 connection requires read-only permissions (`s3:GetObject`, `s3:ListBucket`, `s3:GetBucketLocation`) on `<s3-iceberg-bucket>`.

1. Create a least-privilege IAM policy file `fabric_s3_read_policy.json`:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "FabricOneLakeS3ShortcutRead",
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:ListBucket",
           "s3:GetBucketLocation"
         ],
         "Resource": [
           "arn:aws:s3:::<s3-iceberg-bucket>",
           "arn:aws:s3:::<s3-iceberg-bucket>/*"
         ]
       }
     ]
   }
   ```
2. Create the IAM user and generate an Access Key for the Fabric S3 Connection:
   ```bash
   aws iam create-user --user-name <fabric-s3-iam-user>

   aws iam put-user-policy \
     --user-name <fabric-s3-iam-user> \
     --policy-name <fabric-s3-policy-name> \
     --policy-document file://fabric_s3_read_policy.json

   aws iam create-access-key --user-name <fabric-s3-iam-user>
   ```
   Record the returned `AccessKeyId` (`<aws-access-key-id>`) and `SecretAccessKey` (`<aws-secret-access-key>`).

#### Option B: Microsoft Entra ID Service Principal + AWS IAM OIDC Federation (Secretless / Enterprise Option)
Per Microsoft's official [Integrate Microsoft Entra with AWS S3 shortcuts using service principal authentication](https://learn.microsoft.com/en-us/fabric/onelake/amazon-storage-shortcut-entra-integration) guide, native Amazon S3 connections also support Entra Service Principal authentication with an On-Premises Data Gateway:
1. **In Microsoft Entra ID:**
   - Create an App Registration (`<entra-s3-app-name>`), generate a Client Secret (`<entra-client-secret>`), and record the **Tenant ID** (`<entra-tenant-id>`) and **Application (client) ID** (`<entra-client-id>`).
   - Navigate to **Enterprise applications** $\rightarrow$ search for `<entra-s3-app-name>` $\rightarrow$ copy the **Enterprise Application Object ID** (`<entra-enterprise-sp-object-id>`). *(Critical: Use the Enterprise Application Object ID, NOT the App Registration Object ID).*
2. **In AWS IAM $\rightarrow$ Identity providers:**
   - Add an **OpenID Connect (OIDC)** provider with:
     - **Provider URL:** `https://sts.windows.net/<entra-tenant-id>/`
     - **Audience:** `https://analysis.windows.net/powerbi/connector/AmazonS3`
3. **In AWS IAM $\rightarrow$ Roles:**
   - Create an IAM Role (`<fabric-s3-oidc-role>`) with `fabric_s3_read_policy.json` attached and the following OIDC Trust Policy:
     ```json
     {
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": {
             "Federated": "arn:aws:iam::<aws-account-id>:oidc-provider/sts.windows.net/<entra-tenant-id>/"
           },
           "Action": "sts:AssumeRoleWithWebIdentity",
           "Condition": {
             "StringEquals": {
               "sts.windows.net/<entra-tenant-id>/:aud": "https://analysis.windows.net/powerbi/connector/AmazonS3",
               "sts.windows.net/<entra-tenant-id>/:sub": "<entra-enterprise-sp-object-id>"
             }
           }
         }
       ]
     }
     ```
   - When creating the Fabric Amazon S3 connection in Step 12.2, select **Authentication kind = Service Principal** and supply `<entra-tenant-id>`, `<entra-client-id>`, `<entra-client-secret>`, and `RoleARN = arn:aws:iam::<aws-account-id>:role/<fabric-s3-oidc-role>`.

---

### Step 7: Configure AWS VPC Private Connectivity (Snowflake PrivateLink & S3 VPC Endpoint)

#### 7.1 Create the Amazon S3 VPC Endpoint (Private Route #2)
- **If the OPDG runs on EC2 inside your AWS VPC:** Create a free **S3 Gateway VPC Endpoint** attached to your VPC route table (`<vpc-route-table-id>`):
  ```bash
  aws ec2 create-vpc-endpoint \
    --region <aws-region> \
    --vpc-id <aws-vpc-id> \
    --vpc-endpoint-type Gateway \
    --service-name com.amazonaws.<aws-region>.s3 \
    --route-table-ids <vpc-route-table-id> \
    --tag-specifications 'ResourceType=vpc-endpoint,Tags=[{Key=Name,Value=<s3-vpc-endpoint-name>}]'
  ```
- **If the OPDG runs on-premises or in Azure over VPN/Direct Connect:** Create an **S3 Interface VPC Endpoint** (`--vpc-endpoint-type Interface`) instead, because AWS S3 Gateway Endpoints cannot route across VPN or Direct Connect.

#### 7.2 Configure Snowflake Private Connectivity (Private Route #1)
- **Production (Snowflake Business Critical Edition):**
  1. Run `aws sts get-federation-token --name snowflake-privatelink` in AWS CLI.
  2. In Snowflake (`ACCOUNTADMIN`), run:
     ```sql
     SELECT SYSTEM$AUTHORIZE_PRIVATELINK('<aws-account-id>', '<federated-token-json>');
     SELECT KEY, VALUE FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM$GET_PRIVATELINK_CONFIG())));
     ```
  3. Create an AWS VPC Interface Endpoint for the `privatelink-vpce-id` service name returned by Snowflake, and create a Route 53 Private Hosted Zone for `privatelink.snowflakecomputing.com` pointing `<snowflake-account>.privatelink.snowflakecomputing.com` to the VPC Interface Endpoint's private IP.
- **POC Alternative (Standard/Enterprise Edition without Business Critical PrivateLink):**
  - Use a Snowflake Network Policy (configured in **Step 11**) that restricts login and query traffic exclusively to your EC2 Gateway VM's IP (`<opdg-gateway-ip>/32`).

---

### Step 8: Provision the Windows Server EC2 Instance for the On-Premises Data Gateway

Launch a Windows Server 2022 EC2 instance inside `<aws-vpc-id>` (`<vpc-subnet-id>`):

- **Recommended Sizing & Host Prerequisites ([Install an on-premises data gateway](https://learn.microsoft.com/en-us/data-integration/gateway/service-gateway-install)):**
  - **OS & Runtime:** 64-bit Windows Server 2019 / 2022 / 2025 (Full GUI desktop experience—Server Core and Windows containers are **not** supported) with **.NET Framework 4.8** and at least **4 GB disk space** for gateway logs.
  - **Recommended Hardware (Production):** 8-core CPU, 8+ GB RAM (`m6i.2xlarge` or `c6i.2xlarge` with 16–32 GB RAM and 100 GB gp3 SSD).
  - **POC / Lab Testing:** `t3.large` (2 vCPU, 8 GB RAM) or `t3.small` (2 vCPU, 2 GB RAM + 4 GB Windows pagefile configured via `UserData`—below Microsoft's 8 GB recommendation, suitable only for light POC validation).
- **Security Group & Outbound Network Rules (`<opdg-security-group-id>`):**
  - **Inbound:** **None required for the On-Premises Data Gateway!** (Only allow `TCP 3389` from `<admin-workstation-ip>/32` if you plan to use RDP; or use AWS Systems Manager Session Manager / Run Command with zero inbound ports).
  - **Outbound:** Allow `TCP 443` (HTTPS) to:
    - Snowflake PrivateLink Interface Endpoint security group (or Snowflake endpoint IP range)
    - Amazon S3 VPC Endpoint prefix list (`pl-xxxxxxx`)
    - Microsoft Azure Relay & Fabric control plane / data movement endpoints (`*.servicebus.windows.net`, `*.powerbi.com`, `*.analysis.windows.net`, `*.fabric.microsoft.com`, `*.frontend.clouddatahub.net`, `login.microsoftonline.com`—note that AWS Security Groups filter by IP/CIDR/prefix list rather than FQDN wildcards, so allow outbound `0.0.0.0/0` on `TCP 443` in the EC2 Security Group or enforce FQDN rules via AWS Network Firewall / proxy).
    - *(Note: New gateway installations default to **HTTPS mode** for Azure Relay over `TCP 443`. If direct TCP mode is used instead, outbound `TCP 5671–5672` and `9350–9354` are also required).*

#### 8.1 AWS CLI Command to Launch the Windows Server 2022 EC2 Gateway Instance
```bash
# Look up the latest Windows Server 2022 Full Base AMI in <aws-region>
WIN_AMI_ID=$(aws ssm get-parameter \
  --region <aws-region> \
  --name /aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base \
  --query 'Parameter.Value' --output text)

# Launch the EC2 Gateway instance with an SSM-enabled Instance Profile so Step 9A can run without opening RDP 3389
aws ec2 run-instances \
  --region <aws-region> \
  --image-id "$WIN_AMI_ID" \
  --instance-type t3.large \
  --subnet-id <vpc-subnet-id> \
  --security-group-ids <opdg-security-group-id> \
  --iam-instance-profile Name=<ec2-ssm-instance-profile> \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3","Encrypted":true}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=<opdg-ec2-instance-name>}]'
```

---

### Step 9: Install, Configure & Register the Microsoft On-Premises Data Gateway (OPDG)

You can install and register the gateway either **interactively via GUI** or **headlessly via PowerShell 7 + AWS SSM**.

#### Option A: Headless Automated Installation (PowerShell 7 on the EC2 VM)
```powershell
# 1. Install PowerShell 7.4+ (Required by Microsoft's DataGateway PowerShell module)
New-Item -ItemType Directory -Force -Path C:\OPDG | Out-Null
Invoke-WebRequest -Uri 'https://github.com/PowerShell/PowerShell/releases/download/v7.4.6/PowerShell-7.4.6-win-x64.msi' -OutFile 'C:\OPDG\pwsh7.msi'
Start-Process msiexec.exe -Wait -ArgumentList '/i C:\OPDG\pwsh7.msi /qn'

# 2. Launch PowerShell 7 to install and register the On-Premises Data Gateway
& 'C:\Program Files\PowerShell\7\pwsh.exe' -NoProfile -Command {
    Install-Module -Name DataGateway -Force -AllowClobber -Scope AllUsers
    Import-Module DataGateway

    # Authenticate to your Microsoft Fabric tenant (prints a one-time code for https://login.microsoft.com/device)
    Connect-DataGatewayServiceAccount -ForceDeviceCodeAuthentication:$true

    # Download and silently install the latest official On-Premises Data Gateway service (PBIEgwService)
    Install-DataGateway -AcceptConditions

    # Register the Gateway Cluster in Microsoft Fabric
    # Note: Ensure your Microsoft Fabric / Power BI tenant does NOT have tenant-level
    # Azure Private Link (Block Public Internet Access) enabled when registering from AWS EC2.
    $recoveryKey = ConvertTo-SecureString '<YOUR_GATEWAY_RECOVERY_KEY>' -AsPlainText -Force
    Add-DataGatewayCluster -Name '<opdg-cluster-name>' -RecoveryKey $recoveryKey

    # Verify the Windows service and registered cluster
    Get-Service PBIEgwService
    Get-DataGatewayCluster
}
```

#### Option B: Interactive GUI Installation (via RDP)
1. Download the standard gateway installer from `https://go.microsoft.com/fwlink/?LinkId=2116849` on the EC2 VM.
2. Run the installer $\rightarrow$ accept terms $\rightarrow$ click **Install**.
3. Sign in with your **Microsoft Fabric corporate email** (`<fabric-admin-user>@<your-domain>.com`).
4. Select **Register a new gateway on this computer** $\rightarrow$ click **Next**.
5. Enter:
   - **New on-premises data gateway name:** `<opdg-cluster-name>`
   - **Recovery key:** Enter and confirm a recovery key.
   - **Region:** Confirm it matches your Fabric home tenant region.
6. In the On-Premises Data Gateway desktop app, click the **Network** tab and ensure **HTTPS mode** is **On** (so all Azure Relay communication uses `TCP 443` FQDNs).

---

### Step 10: Verify Line-of-Sight Connectivity from the Gateway VM to Snowflake & S3

Per Microsoft's official [Verify gateway connectivity](https://learn.microsoft.com/en-us/fabric/onelake/create-on-premises-shortcut#verify-gateway-connectivity) guidance, run these checks **from inside the EC2 Gateway VM** before configuring Fabric:

```powershell
# 1A. If using AWS PrivateLink for Snowflake (Step 7.2 Production), verify private DNS & TCP 443:
Resolve-DnsName "<snowflake-account>.privatelink.snowflakecomputing.com"
Test-NetConnection "<snowflake-account>.privatelink.snowflakecomputing.com" -Port 443

# 1B. If using IP-allowlisted public Snowflake endpoint (Step 7.2 POC Alternative):
Test-NetConnection "<snowflake-account>.snowflakecomputing.com" -Port 443

# 2. Verify TCP 443 connectivity & DNS resolution to the Amazon S3 Bucket endpoint
Test-NetConnection "<s3-iceberg-bucket>.s3.<aws-region>.amazonaws.com" -Port 443

# 3. Verify BOTH S3 List AND GetObject (Read) access using the Fabric S3 IAM credentials
#    Get the exact metadata file path returned by SYSTEM$GET_ICEBERG_TABLE_INFORMATION in Step 5
$env:AWS_ACCESS_KEY_ID = "<aws-access-key-id>"
$env:AWS_SECRET_ACCESS_KEY = "<aws-secret-access-key>"
$env:AWS_DEFAULT_REGION = "<aws-region>"

# Test ListBucket:
aws s3 ls s3://<s3-iceberg-bucket>/iceberg/orders_iceberg/ --recursive

# Test GetObject (actual file read of metadata JSON from S3):
aws s3 cp s3://<s3-iceberg-bucket>/iceberg/orders_iceberg/metadata/<actual-metadata-file>.metadata.json C:\OPDG\test_metadata.json
Get-Content C:\OPDG\test_metadata.json -TotalCount 20
```

---

### Step 11: Lock Down the S3 Bucket Firewall & Snowflake Network Policy

Now apply the strict firewall rules so that **only** your AWS VPC Endpoint (`<s3-vpc-endpoint-id>`), your Gateway VM (`<opdg-gateway-ip>`), and **Snowflake's External Volume IAM Role (`<snowflake-iceberg-iam-role>`)** can access S3 and Snowflake:

#### 11.1 Apply the S3 Bucket Policy (`s3_firewall_policy.json`)
*(Critical: In AWS IAM evaluation, an explicit `Deny` overrides any `Allow`. Because Snowflake's compute plane runs in Snowflake's AWS service VPC rather than your AWS VPC, you **must** exempt `<snowflake-iceberg-iam-role>` via `ArnNotEquals` on `aws:PrincipalArn` so the bucket policy does not block Snowflake's own Iceberg metadata/data operations).*

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyAllExceptSnowflakeRoleAndGatewayVPCE",
      "Effect": "Deny",
      "Principal": "*",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::<s3-iceberg-bucket>",
        "arn:aws:s3:::<s3-iceberg-bucket>/*"
      ],
      "Condition": {
        "ArnNotEquals": {
          "aws:PrincipalArn": [
            "arn:aws:iam::<aws-account-id>:role/<snowflake-iceberg-iam-role>",
            "arn:aws:iam::<aws-account-id>:root"
          ]
        },
        "StringNotEqualsIfExists": {
          "aws:SourceVpce": "<s3-vpc-endpoint-id>"
        },
        "NotIpAddressIfExists": {
          "aws:SourceIp": [
            "<opdg-gateway-ip>/32",
            "<admin-workstation-ip>/32"
          ]
        }
      }
    }
  ]
}
```
```bash
aws s3api put-bucket-policy \
  --bucket <s3-iceberg-bucket> \
  --policy file://s3_firewall_policy.json
```

#### 11.2 Apply the Snowflake Network Policy
```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK POLICY <snowflake-gateway-network-policy>
  ALLOWED_IP_LIST = ('<opdg-gateway-ip>/32', '<admin-workstation-ip>/32')
  COMMENT = 'Restrict Snowflake access to the AWS EC2 On-Premises Data Gateway VM';

ALTER USER <snowflake-mirror-user> SET NETWORK_POLICY = <snowflake-gateway-network-policy>;
```

---

### Step 12: Create Both Gateway-Routed Connections in Microsoft Fabric

1. Open **Microsoft Fabric** (`https://app.fabric.microsoft.com`).
2. Click the **Settings (Gear icon)** in the top-right banner $\rightarrow$ select **Manage connections and gateways**.
3. First, click the **On-premises data gateways** tab and verify that **`<opdg-cluster-name>`** shows Status = **Online**.
4. Switch to the **Connections** tab $\rightarrow$ click **+ New** in the top-left ribbon.

#### 12.1 Create Connection #1: Snowflake Connection via OPDG
In the **New connection** pane:
- **Connection type radio button:** Select **On-premises**
- **Gateway cluster name:** Select **`<opdg-cluster-name>`**
- **Connection name:** Enter `<fabric-snowflake-connection-name>`
- **Connection type:** Select **`Snowflake`**
- **Server:**
  - **If using AWS PrivateLink (Step 7.2 Production):** `<snowflake-account>.privatelink.snowflakecomputing.com`
  - **If using IP-allowlisted public endpoint (Step 7.2 POC Alternative):** `<snowflake-account>.snowflakecomputing.com`
  - *(Important: Do NOT include `https://`, and use lowercase for the hostname)*
- **Warehouse:** `<SNOWFLAKE_WAREHOUSE>` *(Important: Match exact uppercase/lowercase casing in Snowflake, e.g., `COMPUTE_WH`)*
- **Authentication method:** Select `Snowflake` (or `KeyPair` / `Microsoft Account`)
- **Username:** `<snowflake-mirror-user>`
- **Password:** `<snowflake-mirror-password>`
- **Privacy level:** `Organizational`
- Click **Create**. Wait for the green banner: *"Created `<fabric-snowflake-connection-name>`."*

#### 12.2 Create Connection #2: Amazon S3 Connection via OPDG
Click **+ New** again in **Manage connections and gateways**:
- **Connection type radio button:** Select **On-premises**
- **Gateway cluster name:** Select **`<opdg-cluster-name>`**
- **Connection name:** Enter `<fabric-s3-connection-name>`
- **Connection type:** Select **`Amazon S3`**
- **Url:** `https://<s3-iceberg-bucket>.s3.<aws-region>.amazonaws.com`
- **Authentication method:** Select `Access Key` (or `Service Principal` if using Entra OIDC federation)
- **Access Key Id:** `<aws-access-key-id>`
- **Secret Access Key:** `<aws-secret-access-key>`
- **Privacy level:** `Organizational`
- Click **Create**. Wait for the green banner: *"Created `<fabric-s3-connection-name>`."*

---

### Step 13: Pre-Validate S3 Connectivity with a Standalone OneLake S3 Shortcut

Before launching the full Snowflake Mirroring wizard, validate that OneLake can read your firewalled S3 bucket through `<opdg-cluster-name>` and `<s3-vpc-endpoint-id>`:

1. Open your Fabric Workspace (`<fabric-workspace-name>`) and open (or create) a test **Lakehouse** (`<fabric-lakehouse-name>`).
2. Right-click **Files** (or **Tables**) in the Lakehouse Explorer $\rightarrow$ select **New shortcut** $\rightarrow$ under **External sources**, choose **Amazon S3**.
3. Under **Existing connection**, select **`<fabric-s3-connection-name>`** (which is bound to `<opdg-cluster-name>`) $\rightarrow$ click **Next**.
4. Browse into `/iceberg/orders_iceberg`, check the folder, and click **Next** $\rightarrow$ **Create**.
5. Expand the shortcut in the Lakehouse Explorer and verify that the exact metadata file returned by `SYSTEM$GET_ICEBERG_TABLE_INFORMATION` in Step 5 (under `/metadata/*.metadata.json`) and the Parquet data files (under `/data/*.parquet`) are visible and readable.

---

### Step 14: Create & Start the Mirrored Snowflake Database in Microsoft Fabric (Portal & REST API)

#### Option A: Via the Microsoft Fabric Portal UI
1. In your Fabric Workspace (`<fabric-workspace-name>`), click **+ New item** $\rightarrow$ select **Mirrored Snowflake**.
2. Enter the name **`<fabric-mirrored-db-name>`** and click **Create**.
3. Under **Existing connections**, select **`<fabric-snowflake-connection-name>`** (routed through `<opdg-cluster-name>`).
4. In the **Database** dropdown, select **`<SNOWFLAKE_DB>`**.
5. On the **Configure mirroring** screen:
   - Select your managed tables (`ACCOUNTS_MANAGED`, `ORDERS_MANAGED`) and your Iceberg table (`ORDERS_ICEBERG`).
   - Because an Iceberg table is selected (or **Mirror all data** including Iceberg is toggled on), Fabric prompts you for the **Storage connection** for the underlying Iceberg storage.
   - Select **`<fabric-s3-connection-name>`** (routed through `<opdg-cluster-name>`).
6. Click **Connect** $\rightarrow$ click **Create mirrored database**.

#### Option B: Via the Microsoft Fabric REST API (`POST /v1/workspaces/{workspaceId}/mirroredDatabases`)
Per Microsoft's official [Mirrored database definition](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/mirrored-database-definition) and [Create Mirrored Database API](https://learn.microsoft.com/en-us/rest/api/fabric/mirroreddatabase/items/create-mirrored-database) documentation:
- The caller must have at least the **Contributor** workspace role and the `MirroredDatabase.ReadWrite.All` or `Item.ReadWrite.All` scope.
- For Snowflake Iceberg mirroring, the S3 storage connection (`<fabric-s3-connection-id>`) **must** be placed under `properties.source.typeProperties.externalStorages[]` with `"type": "AmazonS3"` (not under `target.typeProperties.landingZone`).

1. **Construct the `mirroring.json` definition file:**
```json
{
  "properties": {
    "source": {
      "type": "Snowflake",
      "typeProperties": {
        "connection": "<fabric-snowflake-connection-id>",
        "database": "<SNOWFLAKE_DB>",
        "externalStorages": [
          {
            "type": "AmazonS3",
            "typeProperties": {
              "connection": "<fabric-s3-connection-id>"
            }
          }
        ]
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
      {
        "source": {
          "typeProperties": {
            "schemaName": "<SNOWFLAKE_SCHEMA>",
            "tableName": "ACCOUNTS_MANAGED"
          }
        }
      },
      {
        "source": {
          "typeProperties": {
            "schemaName": "<SNOWFLAKE_SCHEMA>",
            "tableName": "ORDERS_MANAGED"
          }
        }
      },
      {
        "source": {
          "typeProperties": {
            "schemaName": "<SNOWFLAKE_SCHEMA>",
            "tableName": "ORDERS_ICEBERG"
          }
        }
      }
    ]
  }
}
```

2. **Base64-encode `mirroring.json`, create the Mirrored Database item, start mirroring, and poll table replication status:**
```powershell
# Acquire a Microsoft Fabric API token (requires Azure CLI signed into your Fabric tenant)
$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{
    "Authorization" = "Bearer $fabricToken"
    "Content-Type"  = "application/json"
}

# Base64-encode the mirroring.json file
$mirroringJsonBytes = [System.IO.File]::ReadAllBytes(".\mirroring.json")
$mirroringBase64    = [Convert]::ToBase64String($mirroringJsonBytes)

# 1. Create the Mirrored Database item with the InlineBase64 definition envelope
$createBody = @{
    displayName = "<fabric-mirrored-db-name>"
    description = "Mirrored AWS Snowflake database with S3 Iceberg tables via OPDG"
    definition  = @{
        parts = @(
            @{
                path        = "mirroring.json"
                payload     = $mirroringBase64
                payloadType = "InlineBase64"
            }
        )
    }
} | ConvertTo-Json -Depth 10

$createdItem = Invoke-RestMethod `
    -Method Post `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/<fabric-workspace-id>/mirroredDatabases" `
    -Headers $headers `
    -Body $createBody

$mirroredDbId = $createdItem.id
Write-Host "Created Mirrored Database ID: $mirroredDbId"

# 2. Wait until status is no longer 'Initializing', then start mirroring
do {
    Start-Sleep -Seconds 10
    $statusResp = Invoke-RestMethod `
        -Method Post `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/<fabric-workspace-id>/mirroredDatabases/$mirroredDbId/getMirroringStatus" `
        -Headers $headers
    Write-Host "Current Mirroring Status: $($statusResp.status)"
} while ($statusResp.status -eq "Initializing")

if ($statusResp.status -ne "Running") {
    Invoke-RestMethod `
        -Method Post `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/<fabric-workspace-id>/mirroredDatabases/$mirroredDbId/startMirroring" `
        -Headers $headers
    Write-Host "Started Mirroring."
}

# 3. Query table-level replication status (ACCOUNTS_MANAGED, ORDERS_MANAGED, ORDERS_ICEBERG)
$tableStatus = Invoke-RestMethod `
    -Method Post `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/<fabric-workspace-id>/mirroredDatabases/$mirroredDbId/getTablesMirroringStatus" `
    -Headers $headers
$tableStatus.data | Format-Table sourceSchemaName, sourceTableName, status, @{N='processedRows';E={$_.metrics.processedRows}}, @{N='lastSyncDateTime';E={$_.metrics.lastSyncDateTime}}
```

---

### Step 15: Verify Replication Status, Query Managed & Iceberg Tables via T-SQL, Validate Change Propagation, & Enable OneLake Shortcut Caching

1. **Monitor Replication Status:**
   - In the Mirrored Database item (`<fabric-mirrored-db-name>`), view the **Monitor replication** page.
   - Confirm overall **Status = Running** and each table (`ACCOUNTS_MANAGED`, `ORDERS_MANAGED`, and **`ORDERS_ICEBERG`**) shows **Status = Running / Replicating**.
   - *(Note per Microsoft's [Monitor Fabric mirrored database replication](https://learn.microsoft.com/en-us/fabric/mirroring/monitor) documentation: Initial table setup typically takes 2–5 minutes; **Rows replicated** is a cumulative count of inserts/updates/deletes rather than the current table row count; and when no changes occur, Snowflake polling automatically backs off up to 1 hour).*
2. **Query Both Managed Tables and the S3 Iceberg Table via the SQL Analytics Endpoint:**
   - Click **Query in T-SQL** (or switch the top-right dropdown from `Mirrored database` to `SQL analytics endpoint`) and run:
     ```sql
     -- 1. Verify row counts across Managed and Iceberg tables
     SELECT 'ACCOUNTS_MANAGED' AS TABLE_NAME, COUNT(*) AS ROW_CNT FROM [<SNOWFLAKE_SCHEMA>].[ACCOUNTS_MANAGED]
     UNION ALL
     SELECT 'ORDERS_MANAGED'    AS TABLE_NAME, COUNT(*) AS ROW_CNT FROM [<SNOWFLAKE_SCHEMA>].[ORDERS_MANAGED]
     UNION ALL
     SELECT 'ORDERS_ICEBERG'    AS TABLE_NAME, COUNT(*) AS ROW_CNT FROM [<SNOWFLAKE_SCHEMA>].[ORDERS_ICEBERG];

     -- 2. Join the Managed Accounts table with the S3-backed Iceberg Orders table (ORDERS_ICEBERG)
     SELECT c.ACCOUNT_ID,
            c.ACCOUNT_NAME,
            c.SEGMENT,
            i.ORDER_ID,
            i.PRODUCT_NAME,
            i.ORDER_AMOUNT,
            i.ORDER_STATUS,
            i.ORDER_DATE
     FROM [<SNOWFLAKE_SCHEMA>].[ACCOUNTS_MANAGED] AS c
     INNER JOIN [<SNOWFLAKE_SCHEMA>].[ORDERS_ICEBERG] AS i
         ON c.ACCOUNT_ID = i.ACCOUNT_ID
     ORDER BY i.ORDER_ID;
     ```
3. **Validate Live Change Propagation on the Iceberg Table (`ORDERS_ICEBERG`):**
   - In Snowflake, insert a new test record into `ORDERS_ICEBERG`:
     ```sql
     INSERT INTO <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>.ORDERS_ICEBERG VALUES
         (5006, 101, 'Iceberg Incremental Sync Test', 3200.00, 'NEW', CURRENT_DATE());
     ```
   - Wait for the next replication cycle and re-run the T-SQL query in Fabric's SQL analytics endpoint to confirm `ORDER_ID = 5006` appears in `[<SNOWFLAKE_SCHEMA>].[ORDERS_ICEBERG]`.
4. **Enable OneLake Shortcut File Caching (Reduces Repeated AWS S3 Egress Costs):**
   - Open **Workspace settings** $\rightarrow$ **OneLake** $\rightarrow$ **Cache** $\rightarrow$ toggle **Enable cache for shortcuts** to **On** and set the retention period (1 to 28 days).
   - *(Note: Individual files `<= 1 GB` are cached on read and reset their retention window on each access; files `> 1 GB`, first-time reads, and newly written Iceberg files still read from S3).*

---

### Step 16: Automate Direct Lake Semantic Model Creation (OneLake-Scoped Tables, Single & Composite Key Relationships, Descriptions & Synonyms)

Because Fabric Mirroring lands Snowflake tables into OneLake as raw Delta Lake tables, relational **Primary Keys (`PRIMARY KEY`)**, **Foreign Key Relationships (`FOREIGN KEY`)**, **Composite Keys**, **Table/Column `COMMENT` Descriptions**, and **Business Synonyms** are not automatically carried over into a Power BI Semantic Model.

This step automates building and deploying a **Direct Lake Semantic Model** (`<fabric-semantic-model-name>`) directly over the Mirrored Database in OneLake with three strict architectural rules:
1. **Scoped Strictly to OneLake-Mirrored Tables:** Queries `POST .../mirroredDatabases/{mirroredDbId}/getTablesMirroringStatus` first to build an allowlist (`MIRRORED_TABLES`). Any un-mirrored Snowflake tables (such as `INTERNAL_AUDIT_LOGS_UNMIRRORED`) and any foreign keys pointing to un-mirrored tables are automatically excluded.
2. **Extracts Table/Column Descriptions & Synonyms:** Reads `SHOW TABLES IN SCHEMA` and `SHOW COLUMNS IN SCHEMA` from Snowflake, parses `[SYNONYMS: ...]` tags and business comments, and writes them into TMDL `///` descriptions and `SnowflakeSynonyms` annotations for Power BI Copilot / Data Agent AI readiness.
3. **Supports Both Single-Column and Multi-Column (Composite Key) Relationships:** Groups `SHOW IMPORTED KEYS IN SCHEMA` by constraint name (`fk_name`) and `key_sequence`:
   - **Single-Column Foreign Keys (`key_sequence == 1`):** Bound directly in `relationships.tmdl` (e.g., `ORDERS_MANAGED.ACCOUNT_ID -> ACCOUNTS_MANAGED.ACCOUNT_ID` and `ORDERS_MANAGED.REGION_ID -> REGIONS_MANAGED.REGION_ID`).
   - **Multi-Column Composite Keys (`key_sequence >= 2`, e.g., `(TENANT_ID, PRODUCT_CODE)`):** Automatically binds the physical composite surrogate column (`CK_TENANT_PRODUCT` = `TENANT_ID || '|' || PRODUCT_CODE`) in `relationships.tmdl` (`FK_ORDERS_PRODUCTS_COMPOSITE`) **and** generates a pure Direct Lake multi-column `TREATAS` DAX measure (`Total Order Revenue (Composite Key TREATAS)`) that propagates `(TENANT_ID, PRODUCT_CODE)` filters without requiring DAX calculated columns.

#### 16.1 Snowflake SQL: Multi-Table Star Schema with Single FKs, Composite Keys, Descriptions & Synonyms
```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE <SNOWFLAKE_WAREHOUSE>;
USE SCHEMA <SNOWFLAKE_DB>.<SNOWFLAKE_SCHEMA>;

-- 1. Dimension Table #1: ACCOUNTS_MANAGED (Single-Column PK)
CREATE OR REPLACE TABLE ACCOUNTS_MANAGED (
    ACCOUNT_ID INT NOT NULL COMMENT 'Unique primary key identifier for the account [SYNONYMS: Account Number, Client ID]',
    ACCOUNT_NAME STRING COMMENT 'Official registered name of the organization [SYNONYMS: Account, Client Name, Organization Name]',
    SEGMENT STRING COMMENT 'Market tier classification of the account [SYNONYMS: Market Segment, Account Tier]',
    INDUSTRY STRING COMMENT 'Primary industry vertical of the account [SYNONYMS: Sector, Vertical, Business Industry]',
    IS_ACTIVE_FLAG STRING COMMENT 'Indicates whether the account is currently active (Yes/No) [SYNONYMS: Active Status]',
    CREATED_AT TIMESTAMP_LTZ COMMENT 'Timestamp when the account record was onboarded [SYNONYMS: Onboarding Date]',
    CONSTRAINT PK_ACCOUNTS_MANAGED PRIMARY KEY (ACCOUNT_ID) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Master account dimension table containing enterprise and commercial organizations [SYNONYMS: Accounts, Clients, Organizations]';

-- 2. Dimension Table #2: REGIONS_MANAGED (Single-Column PK + Continent > Country > Region Hierarchy)
CREATE OR REPLACE TABLE REGIONS_MANAGED (
    REGION_ID INT NOT NULL COMMENT 'Unique primary key identifier for the sales region [SYNONYMS: Region Key, Territory ID]',
    CONTINENT STRING COMMENT 'Top-level continent name for geographical rollups [SYNONYMS: Geo, Super Region]',
    COUNTRY STRING COMMENT 'Country name within the continent [SYNONYMS: Nation, Country Name]',
    REGION_NAME STRING COMMENT 'Operating sales region name [SYNONYMS: Sales Territory, Region]',
    REGION_MANAGER STRING COMMENT 'Regional lead responsible for the territory [SYNONYMS: Territory Owner, Regional Lead]',
    CONSTRAINT PK_REGIONS_MANAGED PRIMARY KEY (REGION_ID) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Geographic sales territory dimension supporting Continent > Country > Region drill-down hierarchies [SYNONYMS: Regions, Territories, Geography]';

-- 3. Dimension Table #3: PRODUCTS_MANAGED (Multi-Column Composite PK: TENANT_ID + PRODUCT_CODE)
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

-- 4. Fact Table: ORDERS_MANAGED (2 Single-Column FKs + 1 Multi-Column Composite FK)
CREATE OR REPLACE TABLE ORDERS_MANAGED (
    ORDER_ID INT NOT NULL COMMENT 'Unique transaction order identifier [SYNONYMS: Order Number, Transaction ID]',
    ACCOUNT_ID INT NOT NULL COMMENT 'Foreign key referencing ACCOUNTS_MANAGED.ACCOUNT_ID [SYNONYMS: Account Key]',
    REGION_ID INT NOT NULL COMMENT 'Foreign key referencing REGIONS_MANAGED.REGION_ID [SYNONYMS: Region Key]',
    TENANT_ID STRING NOT NULL COMMENT 'Part 1 of composite foreign key referencing PRODUCTS_MANAGED(TENANT_ID, PRODUCT_CODE)',
    PRODUCT_CODE STRING NOT NULL COMMENT 'Part 2 of composite foreign key referencing PRODUCTS_MANAGED(TENANT_ID, PRODUCT_CODE)',
    CK_TENANT_PRODUCT STRING NOT NULL COMMENT 'Physical composite foreign key (TENANT_ID|PRODUCT_CODE) linking to PRODUCTS_MANAGED.CK_TENANT_PRODUCT',
    ORDER_QUANTITY INT COMMENT 'Number of subscription units ordered [SYNONYMS: Quantity, Units]',
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

-- 5. Un-Mirrored Table: INTERNAL_AUDIT_LOGS_UNMIRRORED (Intentionally excluded from Fabric Mirroring to verify OneLake-scoped filtering)
CREATE OR REPLACE TABLE INTERNAL_AUDIT_LOGS_UNMIRRORED (
    AUDIT_ID INT NOT NULL,
    ACCOUNT_ID INT NOT NULL,
    AUDIT_EVENT STRING,
    CONSTRAINT PK_AUDIT_LOGS PRIMARY KEY (AUDIT_ID) NOT ENFORCED,
    CONSTRAINT FK_AUDIT_ACCOUNTS FOREIGN KEY (ACCOUNT_ID) REFERENCES ACCOUNTS_MANAGED (ACCOUNT_ID) NOT ENFORCED
) CHANGE_TRACKING = TRUE
  COMMENT = 'Internal audit table NOT mirrored to OneLake (automatically ignored by Semantic Model generator)';
```

#### 16.2 Python Automation: Generate & Deploy the Direct Lake Semantic Model from Snowflake Metadata (`deploy_direct_lake_semantic_model.py`)
```python
"""
Automated Snowflake Metadata & Relationship -> Fabric Direct Lake Semantic Model Generator
- Filters strictly to tables mirrored into OneLake (via getTablesMirroringStatus)
- Extracts Snowflake Table & Column COMMENT descriptions and [SYNONYMS: ...]
- Extracts Single-Column and Multi-Column (Composite) PK/FK Relationships
- Deploys a Direct Lake Semantic Model via TMDL to Microsoft Fabric REST API and triggers Framing Refresh
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
SEMANTIC_MODEL_NAME = "<fabric-semantic-model-name>"
FABRIC_BEARER_TOKEN = "<fabric-entra-bearer-token>"      # az account get-access-token --resource https://api.fabric.microsoft.com
PBI_BEARER_TOKEN    = "<powerbi-entra-bearer-token>"     # az account get-access-token --resource https://analysis.windows.net/powerbi/api

SNOWFLAKE_ACCOUNT   = "<snowflake-account>"
SNOWFLAKE_USER      = "<snowflake-admin-user>"
SNOWFLAKE_PASSWORD  = "<snowflake-admin-password>"
SNOWFLAKE_DB        = "<SNOWFLAKE_DB>"
SNOWFLAKE_SCHEMA    = "<SNOWFLAKE_SCHEMA>"

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

# 1. DISCOVER TABLES MIRRORED INTO ONELAKE
_, _, tbl_status = fabric_request(
    "POST",
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/mirroredDatabases/{MIRRORED_DB_ID}/getTablesMirroringStatus"
)
mirrored_tables = {
    (t["sourceSchemaName"], t["sourceTableName"])
    for t in tbl_status.get("data", [])
    if t.get("status") in ("Replicating", "Snapshotting", "Initialized")
}

# 2. EXTRACT SNOWFLAKE METADATA, SYNONYMS, PKs & SINGLE/COMPOSITE FKs
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
        data = json.loads(raw.decode("utf-8"))["data"]
        cols = [c["name"] for c in data.get("rowtype", [])]
        return [dict(zip(cols, r)) for r in data.get("rowset", [])]

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
        continue  # Automatically ignore FKs referencing un-mirrored tables
    col_pairs = [(r["fk_column_name"], r["pk_column_name"]) for r in rows_sorted]
    if len(col_pairs) == 1:
        valid_relationships.append({"name": fk_name, "type": "single_column", "from_table": fk_t, "from_column": col_pairs[0][0], "to_table": pk_t, "to_column": col_pairs[0][1], "pairs": col_pairs})
    else:
        fk_cols = {c["name"] for c in table_meta[fk_t]["columns"]}
        pk_cols = {c["name"] for c in table_meta[pk_t]["columns"]}
        ck_col = next(iter(sorted(c for c in (fk_cols & pk_cols) if c.startswith("CK_"))), None)
        valid_relationships.append({"name": fk_name, "type": "composite_key", "from_table": fk_t, "from_column": ck_col, "to_table": pk_t, "to_column": ck_col, "pairs": col_pairs})

# 3. BUILD TMDL PARTS & DEPLOY TO FABRIC SEMANTIC MODELS API
# (Generates definition.pbism, database.tmdl, model.tmdl, expressions.tmdl, relationships.tmdl, and tables/*.tmdl)
```

---

## 5. End-to-End Automation Scripts (Python `boto3`, Snowflake SQL, PowerShell, Fabric REST API)

Below is the complete, corrected Python automation script (`deploy_end_to_end_poc.py`) that enforces proper execution order, raises on SQL/AWS errors, applies least-privilege IAM and S3 bucket policies (exempting Snowflake's External Volume IAM Role), creates the Snowflake database/tables/Iceberg table and mirroring user/role, and deploys/starts the Mirrored Snowflake Database via the official Microsoft Fabric REST API (`externalStorages` schema):

```python
"""
End-to-End Automation Script for AWS + Snowflake Iceberg + Fabric OPDG POC
Replace all <...> placeholders before running.
"""
import base64
import boto3
import botocore.exceptions
import gzip
import json
import time
import urllib.request
import uuid

# ============================================================================
# CONFIGURATION (MASKED PLACEHOLDERS)
# ============================================================================
AWS_ACCESS_KEY             = "<aws-access-key-id>"
AWS_SECRET_KEY             = "<aws-secret-access-key>"
AWS_REGION                 = "<aws-region>"                 # e.g., "us-east-2"
AWS_ACCOUNT_ID             = "<aws-account-id>"             # e.g., "123456789012"
S3_BUCKET_NAME             = "<s3-iceberg-bucket>"
IAM_ROLE_NAME              = "<snowflake-iceberg-iam-role>"
OPDG_GATEWAY_IP            = "<opdg-gateway-ip>"            # e.g., "203.0.113.50"
ADMIN_WORKSTATION_IP       = "<admin-workstation-ip>"       # e.g., "198.51.100.25"

SNOWFLAKE_ACCOUNT          = "<snowflake-account>"          # e.g., "org-account"
SNOWFLAKE_ADMIN_USER       = "<snowflake-admin-user>"       # ACCOUNTADMIN user for initial setup
SNOWFLAKE_ADMIN_PASSWORD   = "<snowflake-admin-password>"
SNOWFLAKE_MIRROR_USER      = "<snowflake-mirror-user>"      # Service user for Fabric Mirroring
SNOWFLAKE_MIRROR_PASSWORD  = "<snowflake-mirror-password>"
SNOWFLAKE_MIRROR_ROLE      = "<SNOWFLAKE_MIRROR_ROLE>"      # e.g., "FABRIC_MIRROR_ROLE"
SNOWFLAKE_WAREHOUSE        = "<SNOWFLAKE_WAREHOUSE>"        # e.g., "COMPUTE_WH"
SNOWFLAKE_DB               = "<SNOWFLAKE_DB>"               # e.g., "FABRIC_ICEBERG_POC_DB"
SNOWFLAKE_SCHEMA           = "<SNOWFLAKE_SCHEMA>"           # e.g., "POC_SCHEMA"
EXTERNAL_VOLUME_NAME       = "<EXTERNAL_VOLUME_NAME>"       # e.g., "EXVOL_FABRIC_ICEBERG_S3"

FABRIC_BEARER_TOKEN        = "<fabric-entra-bearer-token>"  # From: az account get-access-token --resource https://api.fabric.microsoft.com
FABRIC_WORKSPACE_ID        = "<fabric-workspace-id>"
FABRIC_MIRRORED_DB_NAME    = "<fabric-mirrored-db-name>"
FABRIC_SNOWFLAKE_CONN_ID   = "<fabric-snowflake-connection-id>"
FABRIC_S3_CONN_ID          = "<fabric-s3-connection-id>"

# ============================================================================
# 1. INITIALIZE AWS BOTO3 CLIENTS
# ============================================================================
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
s3  = session.client("s3")
iam = session.client("iam")
ec2 = session.client("ec2")

# ============================================================================
# 2. SNOWFLAKE SQL HELPER (WITH EXPLICIT ERROR CHECKING)
# ============================================================================
sf_host = f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
login_req = urllib.request.Request(
    f"{sf_host}/session/v1/login-request",
    data=json.dumps({
        "data": {
            "CLIENT_APP_ID": "PythonConnector",
            "CLIENT_APP_VERSION": "3.0.0",
            "ACCOUNT_NAME": SNOWFLAKE_ACCOUNT,
            "LOGIN_NAME": SNOWFLAKE_ADMIN_USER,
            "PASSWORD": SNOWFLAKE_ADMIN_PASSWORD
        }
    }).encode("utf-8"),
    headers={"Content-Type": "application/json", "Accept": "application/json"}
)
with urllib.request.urlopen(login_req, timeout=15) as resp:
    login_payload = json.loads(resp.read().decode("utf-8"))
    if not login_payload.get("success"):
        raise RuntimeError(f"Snowflake login failed: {login_payload.get('message')}")
    sf_token = login_payload["data"]["token"]

seq_id = 1
def run_snowflake_sql(sql_text):
    global seq_id
    req = urllib.request.Request(
        f"{sf_host}/queries/v1/query-request?requestId={uuid.uuid4()}",
        data=json.dumps({"sqlText": sql_text, "sequenceId": seq_id}).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/snowflake",
            "Authorization": f'Snowflake Token="{sf_token}"'
        }
    )
    seq_id += 1
    with urllib.request.urlopen(req, timeout=60) as qresp:
        raw = qresp.read()
        if qresp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        payload = json.loads(raw.decode("utf-8"))
        if not payload.get("success"):
            raise RuntimeError(f"Snowflake SQL failed: {payload.get('message')}\nSQL: {sql_text}")
        return payload.get("data", {}).get("rowset", [])

# ============================================================================
# 3. CREATE S3 BUCKET (WITH BLOCK PUBLIC ACCESS) & S3 GATEWAY VPC ENDPOINT
# ============================================================================
try:
    if AWS_REGION == "us-east-1":
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
    else:
        s3.create_bucket(
            Bucket=S3_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
        )
except botocore.exceptions.ClientError as e:
    if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
        raise

s3.put_public_access_block(
    Bucket=S3_BUCKET_NAME,
    PublicAccessBlockConfiguration={
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True
    }
)

vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
if not vpcs:
    raise RuntimeError("No default VPC found; specify your target VPC ID explicitly.")
vpc_id = vpcs[0]["VpcId"]
rt_ids = [r["RouteTableId"] for r in ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["RouteTables"]]

existing_vpces = ec2.describe_vpc_endpoints(
    Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "service-name", "Values": [f"com.amazonaws.{AWS_REGION}.s3"]},
        {"Name": "vpc-endpoint-type", "Values": ["Gateway"]}
    ]
)["VpcEndpoints"]

if existing_vpces:
    vpce_id = existing_vpces[0]["VpcEndpointId"]
else:
    vpce_id = ec2.create_vpc_endpoint(
        VpcId=vpc_id,
        VpcEndpointType="Gateway",
        ServiceName=f"com.amazonaws.{AWS_REGION}.s3",
        RouteTableIds=rt_ids
    )["VpcEndpoint"]["VpcEndpointId"]

print(f"VPC: {vpc_id} | S3 Gateway VPC Endpoint: {vpce_id}")

# ============================================================================
# 4. CREATE LEAST-PRIVILEGE IAM ROLE FIRST, THEN EXTERNAL VOLUME & TRUST SYNC
# ============================================================================
initial_trust = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": f"arn:aws:iam::{AWS_ACCOUNT_ID}:root"},
        "Action": "sts:AssumeRole"
    }]
}
snowflake_s3_Least_privilege = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": f"arn:aws:s3:::{S3_BUCKET_NAME}/iceberg/*"
        },
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": f"arn:aws:s3:::{S3_BUCKET_NAME}",
            "Condition": {"StringLike": {"s3:prefix": ["iceberg/*"]}}
        }
    ]
}

try:
    iam.create_role(RoleName=IAM_ROLE_NAME, AssumeRolePolicyDocument=json.dumps(initial_trust))
except iam.exceptions.EntityAlreadyExistsException:
    pass

iam.put_role_policy(
    RoleName=IAM_ROLE_NAME,
    PolicyName="<snowflake-s3-policy-name>",
    PolicyDocument=json.dumps(snowflake_s3_Least_privilege)
)

run_snowflake_sql("USE ROLE ACCOUNTADMIN")
run_snowflake_sql("ALTER ACCOUNT SET PREVENT_UNLOAD_TO_INLINE_URL = FALSE")
run_snowflake_sql(f"""
CREATE OR REPLACE EXTERNAL VOLUME {EXTERNAL_VOLUME_NAME}
  STORAGE_LOCATIONS = (
    (
      NAME = 's3_iceberg_{AWS_REGION.replace("-", "_")}'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://{S3_BUCKET_NAME}/iceberg/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{AWS_ACCOUNT_ID}:role/{IAM_ROLE_NAME}'
    )
  )
  ALLOW_WRITES = TRUE
""")

desc_rows = run_snowflake_sql(f"DESC EXTERNAL VOLUME {EXTERNAL_VOLUME_NAME}")
storage_loc_row = next(r for r in desc_rows if "STORAGE_AWS_IAM_USER_ARN" in str(r))
loc_json = json.loads(next(cell for cell in storage_loc_row if isinstance(cell, str) and "STORAGE_AWS_IAM_USER_ARN" in cell))
sf_iam_user_arn = loc_json["STORAGE_AWS_IAM_USER_ARN"]
sf_external_id  = loc_json["STORAGE_AWS_EXTERNAL_ID"]

trust_policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": sf_iam_user_arn},
        "Action": "sts:AssumeRole",
        "Condition": {"StringEquals": {"sts:ExternalId": sf_external_id}}
    }]
}
iam.update_assume_role_policy(RoleName=IAM_ROLE_NAME, PolicyDocument=json.dumps(trust_policy))
print("Updated IAM Trust Policy; waiting 15s for AWS IAM propagation...")
time.sleep(15)
print("External Volume Verify:", run_snowflake_sql(f"SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('{EXTERNAL_VOLUME_NAME}')"))

# ============================================================================
# 5. CREATE SNOWFLAKE TABLES (MANAGED + ICEBERG) & MIRRORING USER / ROLE
# ============================================================================
run_snowflake_sql(f"CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE} WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
run_snowflake_sql(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
run_snowflake_sql(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DB}")
run_snowflake_sql(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}")

run_snowflake_sql(f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.ACCOUNTS_MANAGED (
    ACCOUNT_ID INT, ACCOUNT_NAME STRING, SEGMENT STRING, COUNTRY STRING, CREATED_AT TIMESTAMP_LTZ
) CHANGE_TRACKING = TRUE
""")
run_snowflake_sql(f"""
INSERT INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.ACCOUNTS_MANAGED VALUES
    (101, 'Account Alpha', 'Enterprise', 'USA', CURRENT_TIMESTAMP()),
    (102, 'Account Bravo', 'Commercial', 'Canada', CURRENT_TIMESTAMP())
""")

run_snowflake_sql(f"""
CREATE OR REPLACE TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.ORDERS_MANAGED (
    ORDER_ID INT, ACCOUNT_ID INT, PRODUCT_NAME STRING, ORDER_AMOUNT DECIMAL(9,2), ORDER_STATUS STRING, ORDER_DATE DATE
) CHANGE_TRACKING = TRUE
""")
run_snowflake_sql(f"""
INSERT INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.ORDERS_MANAGED VALUES
    (5001, 101, 'Analytics Platform Tier 1', 12500.00, 'COMPLETED', CURRENT_DATE()),
    (5002, 102, 'Data Integration Add-On', 4300.50, 'PROCESSING', CURRENT_DATE())
""")

run_snowflake_sql(f"""
CREATE OR REPLACE ICEBERG TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.ORDERS_ICEBERG (
    ORDER_ID INT, ACCOUNT_ID INT, PRODUCT_NAME STRING, ORDER_AMOUNT DECIMAL(9,2), ORDER_STATUS STRING, ORDER_DATE DATE
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = '{EXTERNAL_VOLUME_NAME}'
  BASE_LOCATION = 'orders_iceberg/'
  CHANGE_TRACKING = TRUE
""")
run_snowflake_sql(f"INSERT INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.ORDERS_ICEBERG SELECT * FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.ORDERS_MANAGED")

run_snowflake_sql(f"CREATE ROLE IF NOT EXISTS {SNOWFLAKE_MIRROR_ROLE}")
run_snowflake_sql(f"""
CREATE USER IF NOT EXISTS {SNOWFLAKE_MIRROR_USER}
  PASSWORD = '{SNOWFLAKE_MIRROR_PASSWORD}'
  DEFAULT_ROLE = {SNOWFLAKE_MIRROR_ROLE}
  DEFAULT_WAREHOUSE = {SNOWFLAKE_WAREHOUSE}
  MUST_CHANGE_PASSWORD = FALSE
""")
run_snowflake_sql(f"GRANT ROLE {SNOWFLAKE_MIRROR_ROLE} TO USER {SNOWFLAKE_MIRROR_USER}")
run_snowflake_sql(f"GRANT USAGE ON WAREHOUSE {SNOWFLAKE_WAREHOUSE} TO ROLE {SNOWFLAKE_MIRROR_ROLE}")
run_snowflake_sql(f"GRANT USAGE ON DATABASE {SNOWFLAKE_DB} TO ROLE {SNOWFLAKE_MIRROR_ROLE}")
run_snowflake_sql(f"GRANT USAGE, CREATE STREAM ON SCHEMA {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA} TO ROLE {SNOWFLAKE_MIRROR_ROLE}")
run_snowflake_sql(f"GRANT SELECT ON ALL TABLES IN SCHEMA {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA} TO ROLE {SNOWFLAKE_MIRROR_ROLE}")
run_snowflake_sql(f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA} TO ROLE {SNOWFLAKE_MIRROR_ROLE}")
run_snowflake_sql(f"GRANT USAGE ON EXTERNAL VOLUME {EXTERNAL_VOLUME_NAME} TO ROLE {SNOWFLAKE_MIRROR_ROLE}")

# ============================================================================
# 6. APPLY S3 BUCKET FIREWALL POLICY (EXEMPTING SNOWFLAKE IAM ROLE)
# ============================================================================
s3_firewall_policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "DenyAllExceptSnowflakeRoleAndGatewayVPCE",
        "Effect": "Deny",
        "Principal": "*",
        "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
        "Resource": [f"arn:aws:s3:::{S3_BUCKET_NAME}", f"arn:aws:s3:::{S3_BUCKET_NAME}/*"],
        "Condition": {
            "ArnNotEquals": {
                "aws:PrincipalArn": [
                    f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{IAM_ROLE_NAME}",
                    f"arn:aws:iam::{AWS_ACCOUNT_ID}:root"
                ]
            },
            "StringNotEqualsIfExists": {"aws:SourceVpce": vpce_id},
            "NotIpAddressIfExists": {"aws:SourceIp": [f"{OPDG_GATEWAY_IP}/32", f"{ADMIN_WORKSTATION_IP}/32"]}
        }
    }]
}
s3.put_bucket_policy(Bucket=S3_BUCKET_NAME, Policy=json.dumps(s3_firewall_policy))

# ============================================================================
# 7. CREATE & START FABRIC MIRRORED DATABASE VIA OFFICIAL FABRIC REST API
# ============================================================================
mirroring_definition = {
    "properties": {
        "source": {
            "type": "Snowflake",
            "typeProperties": {
                "connection": FABRIC_SNOWFLAKE_CONN_ID,
                "database": SNOWFLAKE_DB,
                "externalStorages": [{
                    "type": "AmazonS3",
                    "typeProperties": {"connection": FABRIC_S3_CONN_ID}
                }]
            }
        },
        "target": {
            "type": "MountedRelationalDatabase",
            "typeProperties": {
                "defaultSchema": SNOWFLAKE_SCHEMA,
                "format": "Delta",
                "retentionInDays": 7
            }
        },
        "mountedTables": [
            {"source": {"typeProperties": {"schemaName": SNOWFLAKE_SCHEMA, "tableName": "ACCOUNTS_MANAGED"}}},
            {"source": {"typeProperties": {"schemaName": SNOWFLAKE_SCHEMA, "tableName": "ORDERS_MANAGED"}}},
            {"source": {"typeProperties": {"schemaName": SNOWFLAKE_SCHEMA, "tableName": "ORDERS_ICEBERG"}}}
        ]
    }
}

payload_b64 = base64.b64encode(json.dumps(mirroring_definition).encode("utf-8")).decode("utf-8")
create_req_body = {
    "displayName": FABRIC_MIRRORED_DB_NAME,
    "description": "Mirrored AWS Snowflake database with S3 Iceberg tables via OPDG",
    "definition": {
        "parts": [{
            "path": "mirroring.json",
            "payload": payload_b64,
            "payloadType": "InlineBase64"
        }]
    }
}

def fabric_post(url, body=None):
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8") if body is not None else b"",
        headers={
            "Authorization": f"Bearer {FABRIC_BEARER_TOKEN}",
            "Content-Type": "application/json"
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}

created = fabric_post(f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}/mirroredDatabases", create_req_body)
mirrored_db_id = created["id"]
print(f"Created Mirrored Database: {mirrored_db_id}")

while True:
    time.sleep(10)
    st = fabric_post(f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}/mirroredDatabases/{mirrored_db_id}/getMirroringStatus")
    print("Mirroring status:", st.get("status"))
    if st.get("status") != "Initializing":
        break

if st.get("status") != "Running":
    fabric_post(f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}/mirroredDatabases/{mirrored_db_id}/startMirroring")
    print("Triggered startMirroring.")
```

---

## 6. Troubleshooting Matrix & Teardown / Cost-Control Script

### Common Troubleshooting Scenarios

| Symptom / Error | Root Cause | Resolution |
|---|---|---|
| **Mirroring fails or hangs when OPDG / VNet Gateway is selected** | `PREVENT_UNLOAD_TO_INLINE_URL` is set to `TRUE` on the Snowflake account. | Run `ALTER ACCOUNT SET PREVENT_UNLOAD_TO_INLINE_URL = FALSE;` in Snowflake as `ACCOUNTADMIN`. |
| **Connection succeeds, but database/tables do not appear in Fabric** | Casing mismatch on Warehouse, Database, or Schema name. | Snowflake identifiers in Fabric are strictly case-sensitive (`COMPUTE_WH` $\neq$ `compute_wh`). Match exact Snowflake casing. |
| **`Import-Module DataGateway` fails on Windows Server 2022 with PowerShell 5.1 error** | The `DataGateway` PowerShell module (`3000.318+`) requires **PowerShell 7.4+**. | Install `PowerShell-7.4.6-win-x64.msi` and run `Install-DataGateway` inside `pwsh.exe`. |
| **S3 Shortcut returns `403 Forbidden` on SSE-KMS encrypted bucket** | Missing KMS key permissions on the IAM user/role or Bucket Key mismatch. | Grant `kms:Decrypt` and `kms:GenerateDataKey` on the KMS key ARN to `<fabric-s3-iam-user>`. |
| **Snowflake `CREATE ICEBERG TABLE` fails with `not authorized to perform: sts:AssumeRole` on a brand-new AWS Free Plan account** | AWS Free Plan starter accounts belong to an AWS-managed Organization that blocks cross-account `sts:AssumeRole` via an SCP. | Click **Upgrade plan** in AWS Billing (free; keeps your $100 credit) to remove the Free Plan sandbox SCP. |
| **Snowflake `CREATE ICEBERG TABLE` or `SYSTEM$VERIFY_EXTERNAL_VOLUME` fails with `403 Access Denied` after applying Step 11 S3 bucket policy** | S3 Bucket `Deny` policy blocked Snowflake's External Volume IAM Role (`<snowflake-iceberg-iam-role>`), which accesses S3 from Snowflake's service VPC rather than `<s3-vpc-endpoint-id>`. | Include `"ArnNotEquals": {"aws:PrincipalArn": ["arn:aws:iam::<aws-account-id>:role/<snowflake-iceberg-iam-role>", "arn:aws:iam::<aws-account-id>:root"]}` in the S3 bucket `Deny` statement (as shown in Step 11.1). |
| **Fabric Lakehouse preview or Spark fails on Snowflake Iceberg table with `Parquet column cannot be converted ... Expected: decimal(18,4), Found: INT32`** | Known Snowflake Iceberg-to-Delta type-width limitation when `DECIMAL` precision is `>= 10`, `INT64`, or `double`. | Use `DECIMAL(9,2)` (precision `< 10`), preview via the **SQL analytics endpoint** first, or set `spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")` in Spark notebooks. |
| **Recreated Iceberg table (`CREATE OR REPLACE ICEBERG TABLE`) fails to convert in OneLake** | Snowflake preserves older metadata files in S3 for `UNDROP`, leaving multiple metadata sets in the same table folder. | Delete old metadata files under `s3://<s3-iceberg-bucket>/iceberg/<table_folder>/` before recreating the table, or use a fresh `BASE_LOCATION`. |
| **Table replication status shows `Please check your network connectivity to ensure your on-premises data gateway can access <region>.frontend.clouddatahub.net`** | The EC2 Gateway VM is stopped (`PBIEgwService` offline), or outbound `TCP 443` from the EC2 VM to `*.frontend.clouddatahub.net` / Microsoft Fabric relay endpoints is blocked. | Ensure the EC2 Gateway instance is running, `PBIEgwService` is `Running`, outbound `TCP 443` to `*.frontend.clouddatahub.net` is allowed, and restart `PBIEgwService` (`Restart-Service PBIEgwService`). |

### Cost-Control & Full Teardown Notes
*(Important: Stopping the EC2 instance stops hourly EC2 compute charges and takes the OPDG offline, but **does not** eliminate charges for attached EBS volumes (`gp3`), S3 storage, Elastic IPs, Interface VPC Endpoints / NAT Gateways, or active Snowflake/Fabric compute. Remember to also stop Fabric Mirroring or pause capacity when the lab is not in use).*

```python
import boto3

session = boto3.Session(
    aws_access_key_id="<aws-access-key-id>",
    aws_secret_access_key="<aws-secret-access-key>",
    region_name="<aws-region>"
)
ec2 = session.client("ec2")

# Stop the EC2 Gateway instance when not actively running demos
ec2.stop_instances(InstanceIds=["<opdg-ec2-instance-id>"])
print("Stopped EC2 Gateway instance (note: attached EBS volume and S3 storage remain allocated until deleted).")
```

---

## 7. Official Public Documentation References

1. **Microsoft Fabric — Mirroring Snowflake Overview & Firewall Guidance:**  
   https://learn.microsoft.com/en-us/fabric/mirroring/snowflake#mirroring-snowflake-behind-firewall
2. **Microsoft Fabric — Tutorial: Configure Mirrored Databases from Snowflake (Iceberg Storage Connection):**  
   https://learn.microsoft.com/en-us/fabric/mirroring/snowflake-tutorial#start-mirroring-process
3. **Microsoft Fabric — Limitations in Mirrored Databases from Snowflake (`PREVENT_UNLOAD_TO_INLINE_URL`, 1,000-table limit, Iceberg notes):**  
   https://learn.microsoft.com/en-us/fabric/mirroring/snowflake-limitations
4. **Microsoft Fabric — Mirrored Database REST API & `mirroring.json` Definition (`externalStorages` Schema):**  
   https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/mirrored-database-definition  
   https://learn.microsoft.com/en-us/fabric/mirroring/mirrored-database-rest-api
5. **Microsoft Fabric — Use Apache Iceberg Tables in OneLake (Type-Width & Single-Metadata-Set Rules):**  
   https://learn.microsoft.com/en-us/fabric/onelake/onelake-iceberg-tables#iceberg-to-delta-lake
6. **Microsoft Fabric — Create OneLake Shortcuts to Network-Restricted S3/GCS via On-Premises Data Gateway:**  
   https://learn.microsoft.com/en-us/fabric/onelake/create-on-premises-shortcut
7. **Microsoft Fabric — Create an Amazon S3 Shortcut (IAM Permissions & Caching):**  
   https://learn.microsoft.com/en-us/fabric/onelake/create-s3-shortcut
8. **Microsoft Fabric — Integrate Microsoft Entra with AWS S3 Shortcuts Using Service Principal Authentication (OIDC):**  
   https://learn.microsoft.com/en-us/fabric/onelake/amazon-storage-shortcut-entra-integration
9. **Microsoft On-Premises Data Gateway — Installation & Outbound Communication Ports / HTTPS Mode:**  
   https://learn.microsoft.com/en-us/data-integration/gateway/service-gateway-install  
   https://learn.microsoft.com/en-us/data-integration/gateway/service-gateway-communication
10. **Snowflake — AWS PrivateLink and Snowflake:**  
    https://docs.snowflake.com/en/user-guide/admin-security-privatelink
11. **AWS — AWS PrivateLink for Amazon S3 (Gateway vs. Interface VPC Endpoints):**  
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html
