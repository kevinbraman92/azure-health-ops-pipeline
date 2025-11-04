/* sql/02_upsert_objects.sql
   Staging tables, unique constraints, and MERGE-based upsert procs
*/

---------------------------------------
-- 1) Staging tables (truncate-per-run)
---------------------------------------
IF OBJECT_ID('dbo.StgProvider') IS NULL
CREATE TABLE dbo.StgProvider (
  Name        NVARCHAR(200) NOT NULL,
  Region      NVARCHAR(100) NOT NULL,
  Specialty   NVARCHAR(100) NULL
);

IF OBJECT_ID('dbo.StgPatient') IS NULL
CREATE TABLE dbo.StgPatient (
  FirstName   NVARCHAR(100) NOT NULL,
  LastName    NVARCHAR(100) NOT NULL,
  BirthDate   DATE          NULL,
  Gender      NVARCHAR(20)  NULL
);

IF OBJECT_ID('dbo.StgClaim') IS NULL
CREATE TABLE dbo.StgClaim (
  PatientFirstName NVARCHAR(100) NOT NULL,
  PatientLastName  NVARCHAR(100) NOT NULL,
  PatientBirthDate DATE          NULL,
  ProviderName     NVARCHAR(200) NOT NULL,
  ProviderRegion   NVARCHAR(100) NOT NULL,
  AmountBilled     DECIMAL(18,2) NOT NULL,
  AmountPaid       DECIMAL(18,2) NULL,
  Status           NVARCHAR(30)  NOT NULL,
  DateSubmitted    DATE          NOT NULL,
  DatePaid         DATE          NULL
);

---------------------------------------
-- 2) Natural key uniqueness
---------------------------------------
-- Provider uniqueness by (Name, Region)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_Provider_NameRegion')
BEGIN
  CREATE UNIQUE INDEX UX_Provider_NameRegion
  ON dbo.Provider(Name, Region);
END

-- Patient uniqueness by (FirstName, LastName, BirthDate)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_Patient_NameDOB')
BEGIN
  CREATE UNIQUE INDEX UX_Patient_NameDOB
  ON dbo.Patient(FirstName, LastName, BirthDate);
END

-- Claim idempotency: uniqueness by (PatientID, ProviderID, DateSubmitted, AmountBilled)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_Claim_Natural')
BEGIN
  CREATE UNIQUE INDEX UX_Claim_Natural
  ON dbo.Claim(PatientID, ProviderID, DateSubmitted, AmountBilled);
END

---------------------------------------
-- 3) Upsert procs
---------------------------------------

/*
  dbo.sp_upsert_provider
  - MERGE StgProvider → Provider on (Name, Region)
  - Updates Specialty; Inserts new rows
*/
IF OBJECT_ID('dbo.sp_upsert_provider') IS NOT NULL
  DROP PROCEDURE dbo.sp_upsert_provider;
GO
CREATE PROCEDURE dbo.sp_upsert_provider
AS
BEGIN
  SET NOCOUNT ON;

  MERGE dbo.Provider AS tgt
  USING (
    SELECT DISTINCT Name, Region, Specialty
    FROM dbo.StgProvider
  ) AS src
  ON (tgt.Name = src.Name AND tgt.Region = src.Region)
  WHEN MATCHED THEN
    UPDATE SET
      tgt.Specialty = src.Specialty
  WHEN NOT MATCHED THEN
    INSERT (Name, Region, Specialty)
    VALUES (src.Name, src.Region, src.Specialty);

END
GO

/*
  dbo.sp_upsert_patient
  - MERGE StgPatient → Patient on (FirstName, LastName, BirthDate)
  - Updates Gender; Inserts new rows
*/
IF OBJECT_ID('dbo.sp_upsert_patient') IS NOT NULL
  DROP PROCEDURE dbo.sp_upsert_patient;
GO
CREATE PROCEDURE dbo.sp_upsert_patient
AS
BEGIN
  SET NOCOUNT ON;

  MERGE dbo.Patient AS tgt
  USING (
    SELECT DISTINCT FirstName, LastName, BirthDate, Gender
    FROM dbo.StgPatient
  ) AS src
  ON (tgt.FirstName = src.FirstName
      AND tgt.LastName = src.LastName
      AND ISNULL(tgt.BirthDate,'1900-01-01') = ISNULL(src.BirthDate,'1900-01-01'))
  WHEN MATCHED THEN
    UPDATE SET
      tgt.Gender = src.Gender
  WHEN NOT MATCHED THEN
    INSERT (FirstName, LastName, BirthDate, Gender)
    VALUES (src.FirstName, src.LastName, src.BirthDate, src.Gender);

END
GO

/*
  dbo.sp_upsert_claim
  - Resolve PatientID + ProviderID by joining natural keys
  - MERGE into Claim on (PatientID, ProviderID, DateSubmitted, AmountBilled)
  - Updates AmountPaid, Status, DatePaid on change
*/
IF OBJECT_ID('dbo.sp_upsert_claim') IS NOT NULL
  DROP PROCEDURE dbo.sp_upsert_claim;
GO
CREATE PROCEDURE dbo.sp_upsert_claim
AS
BEGIN
  SET NOCOUNT ON;

  WITH Resolved AS (
    SELECT
      p.PatientID,
      pr.ProviderID,
      s.AmountBilled,
      s.AmountPaid,
      s.Status,
      s.DateSubmitted,
      s.DatePaid
    FROM dbo.StgClaim s
    INNER JOIN dbo.Patient p
      ON  p.FirstName = s.PatientFirstName
      AND p.LastName  = s.PatientLastName
      AND ISNULL(p.BirthDate,'1900-01-01') = ISNULL(s.PatientBirthDate,'1900-01-01')
    INNER JOIN dbo.Provider pr
      ON  pr.Name   = s.ProviderName
      AND pr.Region = s.ProviderRegion
  )
  MERGE dbo.Claim AS tgt
  USING (
    SELECT DISTINCT *
    FROM Resolved
  ) AS src
  ON (
    tgt.PatientID    = src.PatientID
    AND tgt.ProviderID   = src.ProviderID
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
