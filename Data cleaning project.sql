SELECT * FROM public.world_layoffs;

--Check for duplicates using cte
WITH layoffs_duplicates
as (SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as duplicates
FROM world_layoffs)
SELECT *
FROM layoffs_duplicates
WHERE duplicates = 2;

--Create a copy of table to perform data analysis operation and have backup
DROP TABLE IF EXISTS world_layoffs_staging;
CREATE TABLE world_layoffs_staging AS SELECT * FROM world_layoffs WHERE FALSE;

ALTER TABLE  world_layoffs_staging
ADD COLUMN row_num INT

INSERT INTO world_layoffs_staging
SELECT *, ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as duplicates
FROM world_layoffs

SELECT * FROM public.world_layoffs_staging

SELECT * FROM world_layoffs_staging
WHERE row_num >=2;


--Delete the duplicates
DELETE 
FROM world_layoffs_staging
WHERE row_num >=2;


--Check company names for issues and mistakes, including using fuzzy matching to find possible misspelling

SELECT DISTINCT company
FROM world_layoffs_staging
ORDER BY 1;

CREATE EXTENSION fuzzystrmatch;

UPDATE world_layoffs_staging
SET company = TRIM(company);

SELECT TRIM(company)
FROM world_layoffs_staging;

SELECT 
    t1.company AS company1,
    t2.company AS company2 
FROM world_layoffs_staging t1
JOIN world_layoffs_staging t2
ON t1.company != t2.company  -- Avoid duplicate pairs and self-matches
WHERE SOUNDEX(t1.company) = SOUNDEX(t2.company)
ORDER BY company1, company2;

CREATE EXTENSION IF NOT EXISTS pg_trgm;


--using similarity and levenshtein to perform fuzzy matching 
WITH CTE as (SELECT DISTINCT t1.company AS company1,
    t2.company AS company2,
    similarity(t1.company, t2.company) AS sim_score,
    levenshtein(t1.company, t2.company) AS edit_distance
FROM world_layoffs_staging t1
JOIN world_layoffs_staging t2
ON t1.company != t2.company
AND t2.ctid > t1.ctid
ORDER BY sim_score DESC, company1, company2)
SELECT *
FROM CTE
WHERE sim_score > 0.5
AND edit_distance <= 3
ORDER BY company1, company2;

--Standardizing data
SELECT * 
FROM world_layoffs_staging
Where 
company ilike 'appgate%';

UPDATE world_layoffs_staging
SET company = 'AppGate'
Where company ilike 'appgate%';

SELECT * 
FROM world_layoffs_staging
Where 
company ilike 'bytedance%'
or company ilike 'clearco%'
or company ilike 'CureFit%'

UPDATE world_layoffs_staging
SET company = CASE
 WHEN company ilike 'appgate%' THEN 'AppGate'
 WHEN company ilike 'bytedance%' THEN 'Bytedance'
 WHEN company ilike 'clearco%' THEN 'Clearco'
 WHEN company ilike 'CureFit%' THEN 'Curefit'
 WHEN company ilike 'salesoft%' THEN 'Salesloft'
 WHEN company ilike 'salesoft%' THEN 'Salesloft'
 ELSE company
END;

SELECT company FROM world_layoffs_staging
WHERE company ilike 'salesloft%';


UPDATE world_layoffs_staging
SET location = TRIM(location);

--Performing fuzzy matching for location column and standardizng data

WITH CTE1 as (SELECT DISTINCT location AS location1
    FROM world_layoffs_staging
	ORDER BY location ASC),
    CTE2 as (SELECT distinct location AS location2
	FROM world_layoffs_staging
	ORDER BY location ASC),
	CTE3 as(
	SELECT DISTINCT location1, location2
	FROM CTE1
	JOIN CTE2
	ON location1 < location2
	ORDER BY location1, location2)
	
SELECT *, 
	similarity(location1, location2) AS sim_score,
    levenshtein(location1, location2) AS edit_distance
	FROM CTE3
	WHERE similarity(location1, location2) > 0.5
    AND levenshtein(location1, location2) <= 2
    ORDER BY location1, location2;

SELECT * FROM public.world_layoffs_staging;

Select distinct location FROM public.world_layoffs_staging
Order by 1;

Select location FROM public.world_layoffs_staging
Where location ilike 'd_ss_ld%'

UPDATE public.world_layoffs_staging
SET location = 'Düsseldorf'
Where location ilike 'd_ss_ld%';

SELECT distinct industry from world_layoffs_staging
order by 1;

UPDATE world_layoffs_staging
SET industry = TRIM(industry);

--Same for industry
WITH CTE1 as (SELECT DISTINCT industry AS industry1
    FROM world_layoffs_staging
	ORDER BY industry ASC),
    CTE2 as (SELECT industry AS industry2
	FROM world_layoffs_staging
	ORDER BY industry ASC),
	CTE3 as(
	SELECT DISTINCT industry1, industry2
	FROM CTE1
	JOIN CTE2
	ON industry1 < industry2
	ORDER BY industry1, industry2)
	
SELECT *, 
	similarity(industry1, industry2) AS sim_score,
    levenshtein(industry1, industry2) AS edit_distance
	FROM CTE3
	WHERE similarity(industry1, industry2) > 0.5
    AND levenshtein(industry1, industry2) <= 7
    ORDER BY industry1, industry2;

SELECT industry from world_layoffs_staging
where industry ilike 'crypto%';

UPDATE world_layoffs_staging
set industry = 'Crypto'
where industry ilike 'crypto%';


SELECT * FROM public.world_layoffs_staging;
SELECT distinct country from world_layoffs_staging
order by 1;

UPDATE public.world_layoffs_staging
SET country = 'United States'
Where country ilike 'united%states%'

-- OR
UPDATE world_layoffs_staging
SET country = TRIM (TRAILING '.' FROM COUNTRY )
Where country ilike 'united%states%';

SELECT date from world_layoffs_staging;

UPDATE world_layoffs_staging
set date = TO_DATE(date, '%MM/%DD/YYYY');

--Changing date type for column containing date as date type
AlTER TABLE world_layoffs_staging
alter COLUMN date TYPE DATE USING date::DATE;

--Checking for null values and and comparing the rows to find out how to populate them
SELECT * FROM world_layoffs_staging
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
from world_layoffs_staging
WHERE industry is NULL
OR industry = '';

SELECT t.company as company1,
t.industry as industry1,
t.location as location1,
r.company as company2,
r.industry as industry2,
r.location as location2
FROM world_layoffs_staging t
JOIN world_layoffs_staging r
ON t.company = r.company
AND
t.industry != r.industry
AND t.ctid<r.ctid
ORDER BY company1;


-- It's work like join without explicit join
UPDATE world_layoffs_staging t1
set industry = t2.industry
from world_layoffs_staging t2
where t1.company = t2.company 
and(t1.industry IS NULL Or t1.industry = '')
and t2.industry is not null
and t1.location = t2.location;


--OR 

UPDATE world_layoffs_staging
SET industry = CASE
   WHEN company like 'Airbnb' THEN 'Travel'
   WHEN company like 'Carvana' Then 'Transportation'
   WHEN company like 'Juul' Then 'Consumer'
ELSE industry
END;  

SELECT * from world_layoffs_staging
where company ilike 'bally%';

SELECT * FROM world_layoffs_staging
WHERE company like 'Airbnb'
    OR company like 'Carvana'
	OR company like 'Juul';


SELECT * FROM world_layoffs_staging;
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM world_layoffs_staging
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM world_layoffs_staging;

ALTER TABLE world_layoffs_staging
DROP COLUMN row_num;