

CREATE TABLE Courts (
    CaseID INT ,           -- 3-digit random number for case ID
    FirstName VARCHAR(255),           -- Offender's First Name
    LastName VARCHAR(255),            -- Offender's Last Name
    DOB DATE,                         -- Offender's Date of Birth
    SSN INT,                          -- Offender's SSN (or any unique identifier)
    CaseStartDate DATETIME,           -- Start date of the case
    CaseEndDate DATETIME,             -- End date of the case (if applicable)
    OffenseCode INT,                  -- Offense Code associated with the case
    OffenseDescription VARCHAR(255)   -- Description of the offense
);

CREATE TABLE Dispositions (
    DispositionID INT PRIMARY KEY,    -- Unique identifier for disposition record
    CaseID INT,                       -- Reference to the Case
    OffenseCode INT,                  -- Reference to the offense code
    DispositionStatus VARCHAR(50),     -- Status of the disposition (e.g., 'Pre-trial', 'Sentenced')
    DispositionDate DATETIME,         -- Date when the disposition was given
);


INSERT INTO Courts (CaseID, FirstName, LastName, DOB, SSN, CaseStartDate, CaseEndDate, OffenseCode, OffenseDescription) VALUES
(101, 'Matt', 'Demon', '1990-05-15',NULL , '2024-12-10', NULL, 12345, 'Battery Theft'),       -- First offense
(101, 'Matt', 'Demon', '1990-05-15',NULL , '2024-12-10', NULL, 67890, 'Assault on people'),    -- Second offense
(101, 'Matt', 'Demon', '1990-05-15',NULL , '2024-12-10', NULL, 23456, 'Car Theft'),            -- Third offense
(102, 'Messi', '', '1992-06-24', NULL, '2024-12-15', '2024-12-18', 67890, 'Assault on people'), -- First offense
(102, 'Messi', '', '1992-06-24', NULL, '2024-12-15', '2024-12-18', 23456, 'Car Theft'),        -- Second offense
(103, 'Yolando', 'Ramirez', '1985-11-08', 123456, '2024-12-12', NULL, 67890, 'Assault on people'); -- First offense



INSERT INTO Dispositions (DispositionID, CaseID, OffenseCode, DispositionStatus, DispositionDate) VALUES
(1, 101, 12345, 'Pre-trial', '2024-12-19'),  -- Disposition for "Battery Theft"
(2, 101, 67890, 'Pre-trial', '2024-12-19'),  -- Disposition for "Assault on people"
(3, 101, 23456, 'Sentenced', '2024-12-19'),  -- Disposition for "Car Theft"
(4, 102, 67890, 'Sentenced', '2024-12-18'),  -- Disposition for "Assault on people"
(5, 102, 23456, 'Sentenced', '2024-12-18'),  -- Disposition for "Car Theft"
(6, 103, 67890, 'Pre-trial', '2024-12-19'),  -- Disposition for "Assault on people"
(7, 101, 12345, 'Sentenced', '2024-12-28'),  -- Disposition for "Battery Theft"
(8, 101, 67890, 'Sentenced', '2024-12-30')  -- Disposition for "Battery Theft"





