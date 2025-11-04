-- sql/00_init_db.sql

-- Create base tables
CREATE TABLE dbo.Provider (
  ProviderID      INT IDENTITY(1,1) PRIMARY KEY,
  Name            NVARCHAR(200) NOT NULL,
  Region          NVARCHAR(100) NOT NULL,
  Specialty       NVARCHAR(100) NULL,
  CreatedAt       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.Patient (
  PatientID       INT IDENTITY(1,1) PRIMARY KEY,
  FirstName       NVARCHAR(100) NOT NULL,
  LastName        NVARCHAR(100) NOT NULL,
  BirthDate       DATE          NULL,
  Gender          NVARCHAR(20)  NULL,
  CreatedAt       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.Claim (
  ClaimID         BIGINT IDENTITY(1,1) PRIMARY KEY,
  PatientID       INT  NOT NULL,
  ProviderID      INT  NOT NULL,
  AmountBilled    DECIMAL(18,2) NOT NULL,
  AmountPaid      DECIMAL(18,2) NULL,
  Status          NVARCHAR(30)  NOT NULL, -- e.g., Submitted/Rejected/Paid
  DateSubmitted   DATE          NOT NULL,
  DatePaid        DATE          NULL,
  CreatedAt       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT FK_Claim_Patient  FOREIGN KEY (PatientID)  REFERENCES dbo.Patient(PatientID),
  CONSTRAINT FK_Claim_Provider FOREIGN KEY (ProviderID) REFERENCES dbo.Provider(ProviderID)
);

-- Useful indexes
CREATE INDEX IX_Claim_DateSubmitted ON dbo.Claim(DateSubmitted);
CREATE INDEX IX_Claim_Status        ON dbo.Claim(Status);
