-- Data Cleaning

SELECT * FROM 
layoffs;

-- Aim of the Project

-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any column

-- Here we have created a new table same as the raw layoffs table
CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT * FROM
layoffs_staging;

-- Here we have inserted all the datas from the layoffs_table to layoffs_staging
INSERT INTO layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM
layoffs_staging;

-- 1. Remove Duplicates


SELECT *,
-- For checking duplicate values
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging ;


-- Creating a temp table with duplicate rows
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * FROM
duplicate_cte
where row_num > 1;

-- Creating a new table as layoffs_staging2 to delete the duplicate rows
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging ;

select * from layoffs_staging2
where row_num>1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

select * from layoffs_staging2
where row_num>1;


-- STANDARDIZING DATA

-- Cleaning the company column
SELECT distinct(TRIM(company))
from layoffs_staging2;

update layoffs_staging2
set company = TRIM(company);

-- Cleaning the industry column
select distinct industry
from layoffs_staging2
order by 1;

select industry 
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select industry 
from layoffs_staging2
where industry like 'Crypto%';

-- Cleaning the country column
select distinct country from
layoffs_staging2
order by 1; 

update layoffs_staging2
set country = 'United States'
where country like 'United States%';

select distinct country from
layoffs_staging2
order by 1; 

-- Cleaning date column

select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

select `date` 
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;



-- Dealing with null and blank values

-- Dealing with industry table

-- replacing the empty row with null
update layoffs_staging2
set industry = null
where industry = '';

select * from layoffs_staging2
where industry is null ;

select * from layoffs_staging2
where company = 'Airbnb';


select t1.industry,t2.industry from 
layoffs_staging2 t1 join
layoffs_staging2 t2 on
	t1.company = t2.company
where (t1.industry is null)
and t2.industry is not null;


update layoffs_staging2 t1 join
layoffs_staging2 t2 on
	t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null)
and t2.industry is not null;


-- where both total_laid_off and percentage_laid_off is null
select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- removing the row_num column because it is no more required

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2;


-- Here we got the required cleaned data