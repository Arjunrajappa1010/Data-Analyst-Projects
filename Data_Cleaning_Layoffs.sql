-- Data cleaning

select *
from layoffs;

-- Steps of data cleaning
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4.  Remove columns / rows that are not necessary 

-- Creating a new staging table
create table layoffs_staging
like layoffs_raw;     -- only column headers will be copied

-- Inserting data into staging table from layoffs_raw
insert layoffs_staging
select *
from layoffs_raw;

select *
from layoffs_staging;

-- Reason to create a stagging table is incase if any mistake happens, it will be easy for us to copy the raw data again from the raw table.
-- It is not recommended to directly work on the raw data table in the real world scenario

-- 1. Removing of dupplicates
-- As we don't have any unique column to use for filter, we are going to partition for each coulmn

Select *,
Row_number() over(
partition by company, location,industry,total_laid_off,percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num  -- Since date is a keyword, we are using backtick to mention it as column name here
from layoffs_staging;

-- Any row_num > 1 are duplicates now
-- Now we need to remove the duplicates

with duplicate_cte as
(
Select *,
Row_number() over(
partition by company, location,industry,total_laid_off,percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num  -- Since date is a keyword, we are using backtick to mention it as column name here
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

-- Now, we have identified the duplicates we cannot directly delete using cte as updates are not possible. So, we are going to add these duplicates into new stagging table and delete them

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
  `row_num` int    -- Here we have added new column row_num 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
Select *,
Row_number() over(
partition by company, location,industry,total_laid_off,percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num  -- Since date is a keyword, we are using backtick to mention it as column name here
from layoffs_staging;

select *
from layoffs_staging2
where row_num >1;


delete
from layoffs_staging2
where row_num >1;

-- Standardizing the data -- means finding the issue and fixing it

-- Removing the extra space from company column
update layoffs_staging2
set company = trim(company);

-- updating same industry name since it will be having extra spaces or no space and all
select distinct industry
from layoffs_staging2
order by 1;
-- we are having cryto, cryptocurrency and crypto Currency which are basically same industry, so we are updating it into Cryto

Select *
from layoffs_staging2
where industry LIKE 'Crypto%';

Update layoffs_staging2
Set industry = 'Crypto'
where industry like 'Crypto%';

-- Checking location if there is any issues
select distinct location
from layoffs_staging2
order by 1;

-- Checking country if there is any issues
select distinct country
from layoffs_staging2
order by 1;
-- here we are having issue where United states mentioned as 2 times with . at the end in one record (United States and United States.)

select distinct country
from layoffs_staging2
where country like ('United States%');

-- Updating it
Update layoffs_staging2
set country = 'United States'
where country like ('United States%'); 

--  we can remove the . using trim as well
-- trim(trailing '.' from country)  --> trailing means after

-- Date column is currrently in text data type, so we need to change it to date datatype

select `date`, str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');  -- converting string to date format  month - day - year 

-- Now changing it into date datatype
alter table layoffs_staging2
modify column `date` date;

-- Step 3: cleaning of null and blank values

-- We are updating the industry which is null with the same company details not null

-- we are updating the empty values to null and then change it to same company details

update layoffs_staging2 
set industry = null
where industry = '';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    where (t1.industry is null or t1.industry ='') and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
     on t1.company=t2.company
     set t1.industry = t2.industry
     where (t1.industry is null or t1.industry ='') and t2.industry is not null;
     
-- checking whethere it got updataed or not

select *
from layoffs_staging2
where company = 'airbnb';

-- Step 4. Removing columns/ rows
-- checking null rows of total_laid_off and percentage_laid_off columns

select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

Delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

select *
from layoffs_staging2;



-- EXPLORATORY DATA ANALYSIS  ( Exploring Data )

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off=1
order by total_laid_off desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;    -- 2 represents sum(total_laid_off)

-- checking the date range of layoffs
select min(`date`), max(`date`)
from layoffs_staging2;

-- country wise layoffs
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- Date and year wise layoffs
select `date`, sum(total_laid_off)
from layoffs_staging2
group by `date`
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

-- stage
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 1 desc;

with rolling_total as
(
select substring(`date`,1,7) as `Month`, sum(total_laid_off) as total_layoff
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, sum(total_layoff) over (order by `month`) as rolling_total
from rolling_total;

with company_year (company, years, total_laid_off)as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as
(
select *, dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select *
from company_year_rank
where Ranking<=5;