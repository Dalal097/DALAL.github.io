SELECT * 
FROM layoffs ;


CREATE TABLE `layoffs_staging4` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text ,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
  ,`ROW_NUM`INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging4 ; 


INSERT INTO layoffs_staging4
 SELECT *, 
ROW_NUMBER() OVER (partition by company , location , industry , total_laid_off 
,percentage_laid_off ,`date`,stage ,country ,funds_raised_millions) as ROW_NUM 
FROM layoffs_staging
; 



select *
from layoffs_staging4 ; 

select *
from layoffs_staging4 
where ROW_NUM = 1 
; 

delete 
from layoffs_staging4
where ROW_NUM != 1 ;

SELECT Company ,trim(company)
from layoffs_staging4 ; 

update layoffs_staging4 
set company = trim(company)
;
SELECT distinct company  
from layoffs_staging3 
 ;
update layoffs_staging4 
set company = 'Paid'
where company like '#Paid' 
;

update layoffs_staging4 
set company = 'Open'
where company like '&Open%' 
;
select *
from layoffs_staging4 ; 


SELECT distinct location   
from layoffs_staging4 
 ;
 
update layoffs_staging4
set location = trim(location);


SELECT distinct industry   
from layoffs_staging4 
;

select  distinct industry   
from layoffs_staging4
where industry like 'crypt%'
;

update layoffs_staging4 
set industry = 'Crypto'
where industry like 'crypt%'
;
update layoffs_staging4
set industry = trim(industry);


select *
from layoffs_staging4 ; 

select  distinct stage   
from layoffs_staging4;

select  distinct country   
from layoffs_staging4
where country like 'united states%'
;

select distinct country , TRIM(trailing '.' from country)
From layoffs_staging3 
order by 1 ; 

update layoffs_staging4 
set country =  TRIM(trailing '.' from country)
where country like 'united states%';


update layoffs_staging4 
set `date` =str_to_date(`date`, '%m/%d/%Y') 
;
alter table layoffs_staging4
modify column `date` date ; 


select *
from layoffs_staging4
where company = 'Airbnb';

delete
from layoffs_staging4 
where total_laid_off is null
and percentage_laid_off is null ;
 

update layoffs_staging4 t1 
join layoffs_staging4 t2 
	on t1.company = t2.company 
set t1.industry =t2.industry 
where (t1.industry is null or t1.industry = ' ')
and t2.industry is not null ;

alter table layoffs_staging4
drop column ROW_NUM ;


select *
from layoffs_staging4 ;



select company ,sum(total_laid_off), max(total_laid_off)
from layoffs_staging4 
group by company
order by 2 desc ; 






select industry ,min(percentage_laid_off)
from layoffs_staging4 
where industry is not null and industry  !=' '
group by industry 
order by 2 asc ; 


select max(`date`),min(`date`)
from layoffs_staging4 
 ; 
 
 
select country ,sum(total_laid_off),max(total_laid_off)
from layoffs_staging4 
group by country  
order by 2 desc ; 

select year (`date`) , sum(total_laid_off)
from layoffs_staging3 
group by year (`date`) 
order by 1 desc ; 


select stage , sum(total_laid_off)  
from layoffs_staging3 
group by stage 
order by 2 desc 
;

select substring(`date`,1,7) as `MONTH` ,sum(total_laid_off)
from layoffs_staging4 
group by `MONTH`
order by 2 desc ; 






select * 
from layoffs_staging4 ; 

with Rolling_Total as 
(
select substring(`date`,1,7) as `MONTH` ,sum(total_laid_off) AS Total_off
from layoffs_staging4 
WHERE substring(`date`,1,7) IS NOT NULL
group by `MONTH`
order by 2 desc 
)
SELECT `MONTH`,Total_off ,sum(total_off) over(order by `MONTH`) as Rolling_total
from Rolling_total ; 


SELECT Company ,year(`date`) , sum(total_laid_off)
from layoffs_staging3 
group by company ,year(`date`) 
order by 3 desc ; 


with company_year(Company,Years,Total_laid_off ) as 
(
SELECT Company ,year(`date`) , sum(total_laid_off)
from layoffs_staging3 
group by company ,year(`date`)
), Company_Year_Rank as
(select * ,dense_rank() over (partition by years order by total_laid_off desc) as Ranking 
from Company_year 
where years is not null 
)
select * 
from Company_Year_Rank
where Ranking <= 5;



 
 