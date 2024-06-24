SELECT *
FROM healthcare_dataset;

-- CREATE STAGING TABLE TO WORK ON

CREATE TABLE HC_staging
LIKE healthcare_dataset;

SELECT *
FROM hc_staging;

INSERT hc_staging
SELECT *
FROM healthcare_dataset;

-- REMOVING DUPLICATES

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY `Name`, Age, Gender, 'Blood Type', 'Medical Condition', 'Date of Admission', Doctor, Hospital, 
'Insurance Provider', 'Billing Amount', 'Room Number', 'Admission Type', 'Discharge Date', Medication, 'Test Results') AS row_num
FROM hc_staging)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

SELECT *
FROM hc_staging
WHERE `Name` LIKE '%ABIgaIL YOung%';

CREATE TABLE `hc_staging2` (
  `Name` text,
  `Age` int DEFAULT NULL,
  `Gender` text,
  `Blood Type` text,
  `Medical Condition` text,
  `Date of Admission` text,
  `Doctor` text,
  `Hospital` text,
  `Insurance Provider` text,
  `Billing Amount` double DEFAULT NULL,
  `Room Number` int DEFAULT NULL,
  `Admission Type` text,
  `Discharge Date` text,
  `Medication` text,
  `Test Results` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM hc_staging2;

INSERT INTO hc_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY `Name`, Age, Gender, 'Blood Type', 'Medical Condition', 'Date of Admission', Doctor, Hospital, 
'Insurance Provider', 'Billing Amount', 'Room Number', 'Admission Type', 'Discharge Date', Medication, 'Test Results') AS row_num
FROM hc_staging;

DELETE
FROM hc_staging2
WHERE row_num > 1;

SELECT hc2.`Name`, hc2.Age 
FROM hc_staging2 hc2
JOIN hc_staging2 hc22
	ON hc2.`Name` = hc22.`Name`
WHERE hc2.Age != hc22.Age;


WITH duplicate_CTE2 AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY `Name`, Age, Gender, 'Blood Type', 'Medical Condition', 'Date of Admission', Doctor, Hospital, 
'Insurance Provider', 'Billing Amount', 'Room Number', 'Admission Type', 'Discharge Date', Medication, 'Test Results') AS row_num2
FROM hc_staging2)
SELECT *
FROM duplicate_CTE2
WHERE row_num2 > 1;

SELECT *
FROM hc_staging2
WHERE `Name` LIKE '%AaROn bAkEr%';

SELECT DISTINCT Hospital
FROM hc_staging2;

SELECT *
FROM hc_staging2
WHERE `Name` = 'aaRoN DaveNpORT';

SELECT `Name`, UCASE(`Name`)
FROM hc_staging2;



-- STANDARDIZING DATA

SELECT `Name`
FROM hc_staging2;



WITH cte_split_first_name AS
(
SELECT 
SUBSTRING(`Name`, 1, LOCATE(' ', `Name`) -1) AS `First`,
SUBSTRING(`Name`, LOCATE(' ', `Name`) +1, LENGTH(`Name`)) AS `Second`
FROM hc_staging2
)
SELECT CONCAT(
UPPER(SUBSTRING(`First`, 1, 1)),
LOWER(SUBSTRING(`First`, 2, LENGTH(`First`)))) AS FirstEdited, CONCAT(
UPPER(SUBSTRING(`Second`, 1, 1)),
LOWER(SUBSTRING(`Second`, 2, LENGTH(`Second`)))) AS SecondEdited
FROM cte_split_first_name;


ALTER TABLE hc_staging2
ADD COLUMN FirstNameEdited text,
ADD COLUMN SecondNameEdited text;

UPDATE hc_staging2
SET FirstNameEdited = SUBSTRING(`Name`, 1, LOCATE(' ', `Name`) -1),
SecondNameEdited = SUBSTRING(`Name`, LOCATE(' ', `Name`) +1, LENGTH(`Name`));

UPDATE hc_staging2
SET FirstNameEdited = CONCAT(
UPPER(SUBSTRING(`FirstNameEdited`, 1, 1)),
LOWER(SUBSTRING(`FirstNameEdited`, 2, LENGTH(`FirstNameEdited`)))),
SecondNameEdited = CONCAT(
UPPER(SUBSTRING(`SecondNameEdited`, 1, 1)),
LOWER(SUBSTRING(`SecondNameEdited`, 2, LENGTH(`SecondNameEdited`))));

UPDATE hc_staging2
SET SecondNameEdited = TRIM(SecondNameEdited);

SELECT SecondNameEdited
FROM hc_staging2
WHERE SecondNameEdited LIKE '% %';

WITH cte_SNE AS
(
SELECT SecondNameEdited,
SUBSTRING(`SecondNameEdited`, 1, LOCATE(' ', `SecondNameEdited`) -1) AS `First`,
SUBSTRING(`SecondNameEdited`, LOCATE(' ', `SecondNameEdited`) +1, LENGTH(`SecondNameEdited`)) AS `Second`
FROM hc_staging2
)
SELECT `Second`, CONCAT(
UPPER(SUBSTRING(`Second`, 1, 1)),
LOWER(SUBSTRING(`Second`, 2, LOCATE(' ', `Second`) - 1)), 
UPPER(SUBSTRING(`Second`, LOCATE(' ', `Second`) + 1))) AS Suffix
FROM cte_SNE
WHERE LOCATE(' ', `Second`) > 1;

ALTER TABLE hc_staging2
ADD COLUMN SecondNameEdited2 text, 
ADD COLUMN SecondNameEdited3 text;

UPDATE hc_staging2
SET SecondNameEdited2 = SUBSTRING(`SecondNameEdited`, 1, LOCATE(' ', `SecondNameEdited`)),
SecondNameEdited3 = SUBSTRING(`SecondNameEdited`, LOCATE(' ', `SecondNameEdited`) +1, LENGTH(`SecondNameEdited`));

UPDATE hc_staging2
SET SecondNameEdited3 = CONCAT(
UPPER(SUBSTRING(SecondNameEdited3, 1, 1)),
LOWER(SUBSTRING(SecondNameEdited3, 2, LOCATE(' ', SecondNameEdited3) - 1)), 
UPPER(SUBSTRING(SecondNameEdited3, LOCATE(' ', SecondNameEdited3) + 1)))
WHERE LOCATE(' ', SecondNameEdited3) > 1;

SELECT DISTINCT SecondNameEdited3, CONCAT(
UPPER(SUBSTRING(SecondNameEdited3, 1, 1)),
LOWER(SUBSTRING(SecondNameEdited3, 2, LENGTH(SecondNameEdited3))))
FROM hc_staging2
WHERE ASCII(SUBSTRING(SecondNameEdited3, 1, 1)) BETWEEN 97 AND 122;

UPDATE hc_staging2
SET SecondNameEdited3 = CONCAT(
UPPER(SUBSTRING(SecondNameEdited3, 1, 1)),
LOWER(SUBSTRING(SecondNameEdited3, 2, LENGTH(SecondNameEdited3))))
WHERE ASCII(SUBSTRING(SecondNameEdited3, 1, 1)) BETWEEN 97 AND 122;

ALTER TABLE hc_staging2
ADD COLUMN FinalNameEdited text;

UPDATE hc_staging2
SET FinalNameEdited = 
CASE 
	WHEN SecondNameEdited2 = '' THEN CONCAT(FirstNameEdited,' ', SecondNameEdited3)
    WHEN SecondNameEdited2 != '' THEN CONCAT(FirstNameEdited,' ', SecondNameEdited2,' ',SecondNameEdited3)
END;

UPDATE hc_staging2
SET FinalNameEdited = REPLACE(FinalNameEdited, 'PHD', 'PhD'),
	FinalNameEdited = REPLACE(FinalNameEdited, 'Phd', 'PhD'),
    FinalNameEdited = REPLACE(FinalNameEdited, 'Ii', 'II'),
    FinalNameEdited = REPLACE(FinalNameEdited, 'Iv', 'IV'),
    FinalNameEdited = REPLACE(FinalNameEdited, 'JR.', 'Jr.'),
    FinalNameEdited = REPLACE(FinalNameEdited, 'Md', 'MD');
    
SELECT *
FROM hc_staging2;

SELECT `Billing Amount`, ROUND(`Billing Amount`, 2) AS BillingAmountRounded
FROM hc_staging2;

ALTER TABLE hc_staging2
ADD COLUMN BillingAmountRounded FLOAT(10, 2);

UPDATE hc_staging2
SET BillingAmountRounded = ROUND(`Billing Amount`, 2);

WITH cte_count_duplicate2 AS
(
SELECT FinalNameEdited,`Medical Condition`,`Blood Type`,`Date of Admission`,`Discharge Date`,`Billing Amount`, COUNT(*) 
FROM hc_staging2
GROUP BY FinalNameEdited,`Medical Condition`,`Blood Type`,`Date of Admission`,`Discharge Date`,`Billing Amount`
HAVING COUNT(*) > 1
)
SELECT COUNT(*)
FROM cte_count_duplicate2;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY FinalNameEdited,`Medical Condition`,`Blood Type`,`Date of Admission`,`Discharge Date`,`Billing Amount`)
AS row_num2
FROM hc_staging2;

SELECT *
FROM hc_staging2	
WHERE FinalNameEdited LIKE 'Adam Hernandez';


CREATE TABLE `hc_staging3` (
  `Name` text,
  `Age` int DEFAULT NULL,
  `Gender` text,
  `Blood Type` text,
  `Medical Condition` text,
  `Date of Admission` text,
  `Doctor` text,
  `Hospital` text,
  `Insurance Provider` text,
  `Billing Amount` double DEFAULT NULL,
  `Room Number` int DEFAULT NULL,
  `Admission Type` text,
  `Discharge Date` text,
  `Medication` text,
  `Test Results` text,
  `row_num` int DEFAULT NULL,
  `FirstNameEdited` text,
  `SecondNameEdited` text,
  `SecondNameEdited2` text,
  `SecondNameEdited3` text,
  `FinalNameEdited` text,
  `BillingAmountRounded` float(10,2) DEFAULT NULL,
  `Duplicates2` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO hc_staging3
SELECT *,
ROW_NUMBER() OVER(PARTITION BY FinalNameEdited,`Medical Condition`,`Blood Type`,`Date of Admission`,`Discharge Date`,`Billing Amount`)
AS Duplicates2
FROM hc_staging2;

SELECT *
FROM hc_staging3
WHERE Duplicates2 > 1;

DELETE
FROM hc_staging3
WHERE Duplicates2 > 1;

CREATE TABLE `Final Cleaned Table` (
  `Name` text,
  `Age` int DEFAULT NULL,
  `Gender` text,
  `Blood Type` text,
  `Medical Condition` text,
  `Date of Admission` text,
  `Doctor` text,
  `Hospital` text,
  `Insurance Provider` text,
  `Billing Amount` double DEFAULT NULL,
  `Room Number` int DEFAULT NULL,
  `Admission Type` text,
  `Discharge Date` text,
  `Medication` text,
  `Test Results` text,
  `row_num` int DEFAULT NULL,
  `FirstNameEdited` text,
  `SecondNameEdited` text,
  `SecondNameEdited2` text,
  `SecondNameEdited3` text,
  `FinalNameEdited` text,
  `BillingAmountRounded` float(10,2) DEFAULT NULL,
  `Duplicates2` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM hc_staging3;

ALTER TABLEhealthcare_dataset hc_staging3
MODIFY FinalNameEdited text AFTER `Name`,
MODIFY BillingAmountRounded float (10,2) AFTER `Billing Amount`;

