

SELECT * 
FROM world_layoffs_staging;

ALTER TABLE world_layoffs_staging
ADD COLUMN STAFF_QUANTITY int;

ALTER TABLE world_layoffs_staging
ALTER COLUMN percentage_laid_off TYPE real using percentage_laid_off::real;


UPDATE world_layoffs_staging t1
SET staff_quantity = (t1.total_laid_off/t2.percentage_laid_off)
FROM world_layoffs_staging t2
WHERE t2.percentage_laid_off is not null and t2.percentage_laid_off !='0'

-- GET THE company with the max total_laid_off, and company with max total_laid_off
-- and percentage_laid_off
WITH total_max (total_laif_off_max, percentage_max) AS(
SELECT 
	MAX(total_laid_off), 
	MAX(percentage_laid_off)
	FROM world_layoffs_staging)
SELECT t1.company, t1.staff_quantity, t1.total_laid_off, t1.percentage_laid_off
FROM world_layoffs_staging t1
JOIN total_max t2
ON t1.total_laid_off = t2.total_laif_off_max
OR t1.percentage_laid_off = t2.percentage_max
WHERE total_laid_off is not null
ORDER BY t1.total_laid_off DESC
LIMIT 2;

SELECT company, staff_quantity, total_laid_off, percentage_laid_off
FROM world_layoffs_staging
WHERE (total_laid_off IN (SELECT MAX(total_laid_off) FROM world_layoffs_staging)
OR percentage_laid_off IN (SELECT MAX(percentage_laid_off)FROM world_layoffs_staging))
AND total_laid_off is not null
ORDER BY total_laid_off desc
LIMIT 2;

SELECT *
FROM world_layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY staff_quantity DESC, total_laid_off DESC

SELECT *
FROM world_layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions desc;

--SELECT the companies that fires the most part of their employees, and have highest
--laid_off in quantity
SELECT company,
	SUM(total_laid_off) as total_laid_off_com, 
	SUM(staff_quantity) as staff_quantity_gen,
    SUM(total_laid_off)::float/SUM(staff_quantity)::float as percentage
FROM world_layoffs_staging
	WHERE percentage_laid_off >= 0.6
	AND total_laid_off is not null
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;


SELECT company,
	SUM(total_laid_off) as total_laid_off_com, 
	SUM(staff_quantity) as staff_quantity_gen,
    SUM(total_laid_off)::float/SUM(staff_quantity)::float as percentage
FROM world_layoffs_staging
	WHERE total_laid_off is not null
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

--WHICH INDUSTRY SUFFERED THE MOST?
SELECT industry,
	SUM(total_laid_off) as total_laid_off_ind, 
	SUM(staff_quantity) as staff_quantity_gen,
    SUM(total_laid_off)::float/SUM(staff_quantity)::float as percentage
FROM world_layoffs_staging
	WHERE total_laid_off is not null
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;

SELECT * 
FROM world_layoffs_staging;

SELECT MIN(date), max(date)
FROM world_layoffs_staging;

SELECT country,
	SUM(total_laid_off) as total_laid_off_coun, 
	SUM(staff_quantity) as staff_quantity_gen,
    SUM(total_laid_off)::float/SUM(staff_quantity)::float as percentage
FROM world_layoffs_staging
	WHERE total_laid_off is not null
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;

SELECT EXTRACT(YEAR FROM date) as year, SUM(total_laid_off) as total_laid_off_year
FROM world_layoffs_staging
GROUP BY year
ORDER BY 2 desc;

SELECT stage, SUM(total_laid_off) as total_laid_off_stage
FROM world_layoffs_staging
GROUP BY stage
ORDER BY 2 desc;

WITH year_laid_off AS(SELECT EXTRACT(YEAR FROM date) as year, 
DATE_PART('month', date) as month, SUM(total_laid_off) as total_laid_off_year
FROM world_layoffs_staging
GROUP BY year, month
ORDER BY 1, 2)
SELECT year, month, 
total_laid_off_year,
SUM(total_laid_off_year) OVER(ORDER BY year, month) as rolling_total
FROM year_laid_off;


SELECT company,
    TO_CHAR(date, 'YYYY'),
	SUM(total_laid_off) as total_laid_off_com, 
	SUM(staff_quantity) as staff_quantity_gen,
    SUM(total_laid_off)::float/SUM(staff_quantity)::float as percentage
FROM world_layoffs_staging
	WHERE total_laid_off is not null
GROUP BY company, TO_CHAR(date, 'YYYY')
ORDER BY company DESC;

WITH Company_year AS (SELECT company,
    TO_CHAR(date, 'YYYY') as years,
	SUM(total_laid_off) as total_laid_off_com, 
	SUM(staff_quantity) as staff_quantity_gen,
    SUM(total_laid_off)::float/SUM(staff_quantity)::float as percentage
FROM world_layoffs_staging
	WHERE total_laid_off is not null
GROUP BY company, TO_CHAR(date, 'YYYY')), company_rank AS(
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off_com desc) ranking
FROM Company_year 
WHERE years is not null)

SELECT* 
FROM company_rank
WHERE ranking <= 5;

CREATE EXTENSION tablefunc;

--PIVOTING USING plpgsql

DO $$
DECLARE
    column_list TEXT;
    query_text TEXT;
BEGIN
    -- Generate column list
    SELECT string_agg(DISTINCT format('"%s" NUMERIC', to_char(date, 'YYYY-MM')), ', ')
    INTO column_list
    FROM world_layoffs_staging
    WHERE date IS NOT NULL;

    IF column_list IS NULL THEN
        RAISE NOTICE 'No valid year-month values found in world_layoffs_staging.date';
        RETURN;
    END IF;

 -- Create temporary table
    EXECUTE format(
        'CREATE TEMP TABLE temp_layoff_pivot (
            country TEXT,
            %s
        )',
        column_list
    );
  
 -- Insert into temporary table
    query_text := format(
        $outer$
        INSERT INTO temp_layoff_pivot
        SELECT *
        FROM crosstab(
            $source$
            WITH rolling_total_c AS (
                SELECT country, to_char(date, 'YYYY-MM') AS Y_M, SUM(total_laid_off) AS total_off
                FROM world_layoffs_staging
                WHERE date IS NOT NULL
                GROUP BY Y_M, country
            ),
            rolling_sums AS (
                SELECT country, Y_M, SUM(total_off) OVER (PARTITION BY country ORDER BY Y_M ASC) AS rolling_total
                FROM rolling_total_c
            )
            SELECT country, Y_M, rolling_total
            FROM rolling_sums
            ORDER BY country, Y_M;
            $source$,
            $categories$
            SELECT DISTINCT to_char(date, 'YYYY-MM') AS Y_M
            FROM world_layoffs_staging
            WHERE date IS NOT NULL
            ORDER BY Y_M;
            $categories$
        ) AS ct (
            country TEXT,
            %s
        );
        $outer$,
        column_list
    );

    EXECUTE query_text;
END;
$$;

-- View results
SELECT * FROM temp_layoff_pivot;


DROP TABLE IF EXISTS industry_lay_off;
DO $$
DECLARE
 column_list TEXT;
 query_text TEXT;
BEGIN
-- Column list
 SELECT string_agg(DISTINCT format('"%s" Numeric', TO_CHAR(date, 'YYYY')),', ')
 INTO column_list
 FROM world_layoffs_staging
 WHERE date is not null;

 IF column_list is null THEN
 RAISE NOTICE 'No valid date values';
 RETURN;
 END IF;

 -- Create temporary table
 EXECUTE format('CREATE TEMPORARY TABLE industry_lay_off(
industry TEXT,
%s)', column_list);

-- Insert into temporary table
 query_text:= format(
$outer$
INSERT INTO industry_lay_off
SELECT * 
FROM crosstab(
$source$
WITH industry_total AS( Select industry, 
TO_CHAR(date, 'YYYY') as year_m, 
SUM(total_laid_off) as total_laid_off
FROM world_layoffs_staging
GROUP BY industry, year_m),
industry_rolling_total AS (Select *, 
sum (total_laid_off) OVER(Partition by industry ORDER by year_m) as rolling_total
FROM industry_total)
SELECT industry, year_m, rolling_total
FROM industry_rolling_total
ORDER BY industry, year_m
$source$,
$categories$
SELECT DISTINCT to_char(date, 'YYYY') AS year_m
FROM world_layoffs_staging
WHERE date is not null
order by year_m;
$categories$
) AS ct (
industry TEXT,
%s
);
$outer$,
column_list
);

 EXECUTE query_text;
END;
$$;

SELECT * FROM industry_lay_off;
