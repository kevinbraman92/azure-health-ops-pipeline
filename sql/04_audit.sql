/* =================================================================
   04_audit.sql
   - Run-level audit tables
   - Add RunID support to Reject_Claim
   - Summary view
   ================================================================= */

------------------------------------------------------------
-- 1) ETL run header table
------------------------------------------------------------
IF OBJECT_ID('dbo.ETL_Run') IS NULL
BEGIN
  CREATE TABLE dbo.ETL_Run
  (
      RunID               BIGINT IDENTITY(1,1) PRIMARY KEY,
      StartedAt           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
      EndedAt             DATETIME2 NULL,
      Status              NVARCHAR(20) NULL,     -- 'SUCCESS' | 'FAILED' | 'PARTIAL'

      -- Staging row counts (snapshotted before merge)
      StgProviderCount    INT NULL,
      StgPatientCount     INT NULL,
      StgClaimCount       INT NULL,

      -- Final row counts (after merge)
      ProviderCount       INT NULL,
      PatientCount        INT NULL,
      ClaimCount          INT NULL,

      -- DQ totals (after merge)
      RejectTotal         INT NULL
  );
  CREATE INDEX IX_ETL_Run_StartedAt ON dbo.ETL_Run(StartedAt DESC);
END
GO

------------------------------------------------------------
-- 2) Ensure Reject_Claim has RunID
------------------------------------------------------------
IF COL_LENGTH('dbo.Reject_Claim','RunID') IS NULL
BEGIN
  ALTER TABLE dbo.Reject_Claim ADD RunID BIGINT NULL;
  CREATE INDEX IX_Reject_Claim_RunID ON dbo.Reject_Claim(RunID);
END
GO

------------------------------------------------------------
-- 3) Summary view (latest 50 runs + DQ breakdown)
------------------------------------------------------------
IF OBJECT_ID('dbo.vw_ETL_Run_Summary') IS NOT NULL
  DROP VIEW dbo.vw_ETL_Run_Summary;
GO
CREATE VIEW dbo.vw_ETL_Run_Summary
AS
SELECT TOP (50)
    r.RunID,
    r.StartedAt,
    r.EndedAt,
    r.Status,
    r.StgProviderCount, r.StgPatientCount, r.StgClaimCount,
    r.ProviderCount, r.PatientCount, r.ClaimCount,
    r.RejectTotal
FROM dbo.ETL_Run r
ORDER BY r.RunID DESC;
GO

IF OBJECT_ID('dbo.vw_ETL_Run_DQ_Breakdown') IS NOT NULL
  DROP VIEW dbo.vw_ETL_Run_DQ_Breakdown;
GO
CREATE VIEW dbo.vw_ETL_Run_DQ_Breakdown
AS
SELECT
    rc.RunID,
    rc.RuleCode,
    COUNT(*) AS RejectCount
FROM dbo.Reject_Claim rc
GROUP BY rc.RunID, rc.RuleCode;
GO
