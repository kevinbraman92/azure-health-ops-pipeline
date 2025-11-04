/* ================================================================
   03_data_quality.sql
   - Adds a reject table for bad claims (data quality)
   - Replaces sp_upsert_claim with a DQ-aware version
   - Adds a summary view for quick monitoring
   ================================================================= */

------------------------------------------------------------
-- 1) Reject table to capture invalid claim rows
------------------------------------------------------------
IF OBJECT_ID('dbo.Reject_Claim') IS NULL
BEGIN
  CREATE TABLE dbo.Reject_Claim
  (
      RejectID           BIGINT IDENTITY(1,1) PRIMARY KEY,
      IngestedAt         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
      RuleCode           NVARCHAR(50) NOT NULL,     -- e.g., FK_NOT_FOUND, NEGATIVE_AMOUNT
      Reason             NVARCHAR(4000) NULL,

      -- Original staging columns (natural keys + facts)
      PatientFirstName   NVARCHAR(100) NOT NULL,
      PatientLastName    NVARCHAR(100) NOT NULL,
      PatientBirthDate   DATE          NULL,
      ProviderName       NVARCHAR(200) NOT NULL,
      ProviderRegion     NVARCHAR(100) NOT NULL,
      AmountBilled       DECIMAL(18,2) NOT NULL,
      AmountPaid         DECIMAL(18,2) NULL,
      Status             NVARCHAR(30)  NOT NULL,
      DateSubmitted      DATE          NOT NULL,
      DatePaid           DATE          NULL,

      -- Diagnostics (what we resolved, if any)
      ResolvedPatientID  INT           NULL,
      ResolvedProviderID INT           NULL
  );

  -- Helpful indexes for triage
  CREATE INDEX IX_Reject_Claim_IngestedAt ON dbo.Reject_Claim(IngestedAt);
  CREATE INDEX IX_Reject_Claim_RuleCode   ON dbo.Reject_Claim(RuleCode);
END
GO


------------------------------------------------------------
-- 2) Replace the MERGE proc with a DQ-aware version
------------------------------------------------------------
IF OBJECT_ID('dbo.sp_upsert_claim') IS NOT NULL
  DROP PROCEDURE dbo.sp_upsert_claim;
GO

CREATE PROCEDURE dbo.sp_upsert_claim
AS
BEGIN
  SET NOCOUNT ON;

  /* Base join attempt: try to resolve PatientID & ProviderID by natural keys */
  WITH JoinBase AS (
    SELECT
      s.*,
      p.PatientID,
      pr.ProviderID
    FROM dbo.StgClaim s
    LEFT JOIN dbo.Patient p
      ON  p.FirstName = s.PatientFirstName
      AND p.LastName  = s.PatientLastName
      AND ISNULL(p.BirthDate,'1900-01-01') = ISNULL(s.PatientBirthDate,'1900-01-01')
    LEFT JOIN dbo.Provider pr
      ON  pr.Name   = s.ProviderName
      AND pr.Region = s.ProviderRegion
  )

  /* ============== DQ Rejects ============== */
  -- 1) Missing FKs (patient or provider not found)
  INSERT INTO dbo.Reject_Claim
  (
    RuleCode, Reason,
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    ResolvedPatientID, ResolvedProviderID
  )
  SELECT
    'FK_NOT_FOUND' AS RuleCode,
    CONCAT('Could not resolve ',
           CASE WHEN PatientID  IS NULL THEN 'Patient ' ELSE '' END,
           CASE WHEN ProviderID IS NULL THEN 'Provider ' ELSE '' END,
           'from natural keys.') AS Reason,
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    PatientID, ProviderID
  FROM JoinBase
  WHERE PatientID IS NULL OR ProviderID IS NULL;

  /* For the remaining rules, only evaluate rows where both FKs resolved */
  ;WITH FKResolved AS (
    SELECT * FROM JoinBase
    WHERE PatientID IS NOT NULL AND ProviderID IS NOT NULL
  )
  -- 2) Negative amounts
  INSERT INTO dbo.Reject_Claim
  (
    RuleCode, Reason,
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    ResolvedPatientID, ResolvedProviderID
  )
  SELECT
    'NEGATIVE_AMOUNT',
    'AmountBilled and/or AmountPaid is negative.',
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    PatientID, ProviderID
  FROM FKResolved
  WHERE (AmountBilled < 0) OR (AmountPaid < 0);

  -- 3) Overpaid (AmountPaid > AmountBilled)
  INSERT INTO dbo.Reject_Claim
  (
    RuleCode, Reason,
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    ResolvedPatientID, ResolvedProviderID
  )
  SELECT
    'OVERPAID',
    'AmountPaid exceeds AmountBilled.',
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    PatientID, ProviderID
  FROM FKResolved
  WHERE AmountPaid IS NOT NULL AND AmountBilled IS NOT NULL
    AND AmountPaid > AmountBilled;

  -- 4) Date order invalid (DatePaid before DateSubmitted)
  INSERT INTO dbo.Reject_Claim
  (
    RuleCode, Reason,
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    ResolvedPatientID, ResolvedProviderID
  )
  SELECT
    'DATE_ORDER',
    'DatePaid is earlier than DateSubmitted.',
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    PatientID, ProviderID
  FROM FKResolved
  WHERE DatePaid IS NOT NULL AND DateSubmitted IS NOT NULL
    AND DatePaid < DateSubmitted;

  -- 5) Bad status (enforce simple domain)
  INSERT INTO dbo.Reject_Claim
  (
    RuleCode, Reason,
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    ResolvedPatientID, ResolvedProviderID
  )
  SELECT
    'BAD_STATUS',
    'Status not in (Submitted, Rejected, Paid).',
    PatientFirstName, PatientLastName, PatientBirthDate,
    ProviderName, ProviderRegion,
    AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid,
    PatientID, ProviderID
  FROM FKResolved
  WHERE UPPER(Status) NOT IN ('SUBMITTED','REJECTED','PAID');

  /* ============== Valid set for MERGE ============== */
  ;WITH Valid AS (
    SELECT DISTINCT
      PatientID, ProviderID,
      AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid
    FROM JoinBase jb
    WHERE
      -- must have both FKs resolved
      jb.PatientID  IS NOT NULL AND
      jb.ProviderID IS NOT NULL
      -- pass all checks
      AND (jb.AmountBilled IS NOT NULL AND jb.AmountBilled >= 0)
      AND (jb.AmountPaid   IS NULL OR jb.AmountPaid >= 0)
      AND NOT (jb.AmountPaid IS NOT NULL AND jb.AmountBilled IS NOT NULL AND jb.AmountPaid > jb.AmountBilled)
      AND (jb.DateSubmitted IS NOT NULL)
      AND NOT (jb.DatePaid IS NOT NULL AND jb.DateSubmitted IS NOT NULL AND jb.DatePaid < jb.DateSubmitted)
      AND (UPPER(jb.Status) IN ('SUBMITTED','REJECTED','PAID'))
  )
  MERGE dbo.Claim AS tgt
  USING Valid AS src
  ON (
    tgt.PatientID      = src.PatientID
    AND tgt.ProviderID = src.ProviderID
    AND tgt.DateSubmitted = src.DateSubmitted
    AND tgt.AmountBilled  = src.AmountBilled
  )
  WHEN MATCHED THEN
    UPDATE SET
      tgt.AmountPaid = src.AmountPaid,
      tgt.Status     = src.Status,
      tgt.DatePaid   = src.DatePaid
  WHEN NOT MATCHED THEN
    INSERT (PatientID, ProviderID, AmountBilled, AmountPaid, Status, DateSubmitted, DatePaid)
    VALUES (src.PatientID, src.ProviderID, src.AmountBilled, src.AmountPaid, src.Status, src.DateSubmitted, src.DatePaid);

END
GO


------------------------------------------------------------
-- 3) Summary view for monitoring / dashboards
------------------------------------------------------------
IF OBJECT_ID('dbo.vw_DQ_Claim_Summary') IS NOT NULL
  DROP VIEW dbo.vw_DQ_Claim_Summary;
GO

CREATE VIEW dbo.vw_DQ_Claim_Summary
AS
SELECT
  CONVERT(date, IngestedAt)              AS IngestDate,
  RuleCode,
  COUNT(*)                               AS RejectCount
FROM dbo.Reject_Claim
GROUP BY CONVERT(date, IngestedAt), RuleCode;
GO
