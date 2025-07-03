-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #1. Create a table, load data, and query data

-- COMMAND ----------


CREATE TABLE C_amazon_categories(
  id BIGINT NOT NULL PRIMARY KEY,
  category_name STRING NOT NULL,
  category_type STRING NOT NULL
) USING DELTA;

-- COMMAND ----------

INSERT INTO P_amazon_products 
SELECT 
    CAST(asin AS STRING) AS asin,
    CAST(title AS STRING) AS title,
    CAST(imgUrl AS STRING) AS imgUrl,
    CAST(productURL AS STRING) AS productURL,
    CAST(stars AS DECIMAL(3,2)) AS stars,
    CAST(reviews AS BIGINT) AS reviews,
    CAST(price AS DECIMAL(10,2)) AS price,
    CAST(listPrice AS DECIMAL(10,2)) AS listPrice,
    CAST(category_id AS INT) AS category_id,
    CAST(isBestSeller AS BOOLEAN) AS isBestSelling,
    CAST(boughtInLastMonth AS BIGINT) AS boughtInlastMonth
FROM amazon_products_ss;

-- COMMAND ----------

-- Load data into one place with schema definitions
show tables;

-- COMMAND ----------

DESCRIBE amazon_categories_with_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Transform and load raw data

-- COMMAND ----------

INSERT INTO C_amazon_categories 
SELECT 
    CAST(id AS INT) AS id,
    CAST(category_name AS STRING) AS category_name,
    CAST(category_type AS STRING) AS category_type
FROM amazon_categories_with_type;

-- COMMAND ----------

-- Check schema
DESCRIBE amazon_products_ss;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Raw data validation

-- COMMAND ----------

SELECT COUNT(*) FROM P_amazon_products;

-- COMMAND ----------

-- 1. Check the basic number
SELECT COUNT(*) FROM P_amazon_products;

-- 2.NULL 값 체크
SELECT 
    COUNT(*) as total_rows,
    COUNT(asin) as non_null_asin,
    COUNT(title) as non_null_title,
    COUNT(price) as non_null_price
FROM P_amazon_products;

-- 3.Check sample data
SELECT * FROM P_amazon_products LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Check out best sellers by product

-- COMMAND ----------

select
  p.asin,
  p.title,
  p.isBestSelling,
  p.category_id,
  c.category_name,
  c.category_type
from p_amazon_products p
inner join c_amazon_categories c on p.category_id = c.id -- After matching the product and category, based on the products that exist on both sides
order by p.asin; -- using aggregate functions
  

-- COMMAND ----------

select
  category_type,
  count(*) as total_products,
  sum(case when isBestSelling = true then 1 else 0 end) as --How many best sellers are there in the category?
  best_selling_products,
  round(sum(case when isBestSelling = true then 1 else 0 end) * 100.0 / count(*),2)
  as best_selling_ration -- bestseller and ratio
  from (
    select
      p.asin,
      p.title,
      p.isBestSelling,
      p.category_id,
      c.category_name,
      c.category_type
    from p_amazon_products p
    inner join c_amazon_categories c on p.category_id = c.id -- After matching the/ both table information

  )products_with_category
  group by category_type
  order by total_products desc;

  select
  category_type,
  count(*) as total_products,
  sum(case when isBestSelling = true then 1 else 0 end) as --How many best sellers are there in the category?
  best_selling_products,
  round(sum(case when isBestSelling = true then 1 else 0 end) * 100.0 / count(*),2)
  as best_selling_ration -- bestseller and ratio
  from (
    select
      p.asin,
      p.title,
      p.isBestSelling,
      p.category_id,
      c.category_name,
      c.category_type
    from p_amazon_products p
    inner join c_amazon_categories c on p.category_id = c.id -- After matching the/ both table information

  )products_with_category
  group by category_type
  order by total_products desc;

  



-- COMMAND ----------

-- MAGIC %md
-- MAGIC > ### first look at the price range distribution by category_type.

-- COMMAND ----------

select
  c.category_type,
  case 
     when p.price = 0 then  'free(0)'
     when p.price <= 20 then '0.01~20'
     when p.price <= 50 then '20~50'
     when p.price <= 100 then '50~100'
     when p.price <= 200 then '100~200'
    else '200+'
  end as price_range,
  count(*) as products_count,
  round(count(*) * 100.0 / sum(count(*)) over(partition by c.category_type),2)
from p_amazon_products p
join c_amazon_categories c on p.category_id = c.id
where p.price is not null and p.price >= 0
group by c.category_type,
 case  
     when p.price = 0 then  'free(0)'
     when p.price <= 20 then '0.01~20'
     when p.price <= 50 then '20~50'
     when p.price <= 100 then '50~100'
     when p.price <= 200 then '100~200'
    else '200+'
  end
order by c.category_type, price_range;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### category_type standard deviation

-- COMMAND ----------

select
  c.category_type,
  count(*) as total_products,
  round(avg(p.price),2) as avg_price,
  round(stddev(p.price),2) as std_deviation,
  min(p.price) as min_price,
  max(p.price) as max_price,
  round((stddev(p.price)/avg(p.price))*100,2) as coefficient_of_variation,
  round(max(p.price) - min(p.price), 2) as price_range
from p_amazon_products p
join c_amazon_categories c on p.category_id = c.id
where p.price is not null and p.price >= 0
group by c.category_type
order by std_deviation desc;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Identify products with a rating of 4.0 or higher and a review count in the bottom 30%

-- COMMAND ----------

select 
    a.category_type,
    p.*
from amazon_products_ss p
left join amazon_categories_with_type as a on p.category_id = a.id
where p.reviews <= (
    -- number of reviews is 30% cutoff
    select min(reviews)
    from (
        -- setting the 100th percentile
        select reviews,
               ntile(100) over (order by reviews) as percentile
        from amazon_products_ss
        where reviews is not null
    ) t
    where t.percentile <= 30
)
and p.stars >= 4.0 -- 4.0over

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Calculate the percentage of hidden gem products by category
-- MAGIC -Hidden Gems: Products with Low Reviews but High Ratings
-- MAGIC > Analysis is conducted on products with "lower 30% of reviews and a rating of 4.0 or higher"

-- COMMAND ----------

with base as (
	select
		a.*,
		b.category_type
	from (
		select *,
			row_number() over (ORDER BY asin) as asin_seq
		from amazon_products_ss
	) a
	left join amazon_categories_with_type as b on a.category_id = b.id
),

target as (
	select 
		asin_seq
	from base p
	where p.reviews <= (
		-- cutoff 30%
		select min(reviews)
		from (
			select 
				reviews,
				ntile(100) over (order by reviews) as percentile
			from base
			where reviews is not null
		) t
		where t.percentile <= 30
	)
	and p.stars >= 4.0
)

select
	a.category_type,
	count(a.asin_seq) as ea, -- total number of products
	count(case when b.asin_seq is not null then 1 end) as tea -- hidden gem number
from base as a
left join target as b on a.asin_seq = b.asin_seq
group by a.category_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analyze whether it is a bestseller
-- MAGIC * X-axis score (market entry barrier): Normalize < bestseller ratio + total product reviews > 
-- MAGIC * Y-axis score (profit potential score): Normalize < total price + average price + average rating > and sum

-- COMMAND ----------


with category_base  as (
	select
		category_type,
		total_products,
		bestseller_count,
		(bestseller_count / total_products) as bestseller_ratio, -- number of bestsellers by category
		total_reviews, -- total product reviews
		
		(total_reviews / total_products) as avg_reviews, -- reviews per product
		sum_price, -- total price
		avg_price, -- average price
		avg_stars -- average rating
	from(
		select
			c.category_type,	
			count(*) as total_products, -- otal products by category
			sum(case when p.isBestSeller = true then 1 else 0 end) as bestseller_count, -- number of bestsellers by category
			sum(p.reviews) as total_reviews, -- total product reviews
			
			sum(p.price) as sum_price, -- total price
			avg(p.price) as avg_price, -- average price
			avg(p.stars) as avg_stars -- average rating
		from amazon_products_ss as p
	   left join amazon_categories_with_type as c on p.category_id = c.id
		where p.reviews is not null and p.price is not null and p.stars is not null
		group by c.category_type
	) a
)

, agg_stats as (
-- Overall product averages and standard deviations (for comparing category-specific values)
  select
    avg((bestseller_count * 1.0) / total_products) as avg_bsr,
    stddev_pop((bestseller_count * 1.0) / total_products) as std_bsr,

    avg((total_reviews * 1.0) / total_products) as avg_reviews,
    stddev_pop((total_reviews * 1.0) / total_products) as std_reviews,

    avg(sum_price) as avg_sum_price,
    stddev_pop(sum_price) as std_sum_price,

    avg(avg_stars) as avg_avg_stars,
    stddev_pop(avg_stars) as std_avg_stars
  from category_base
)


select
  a.category_type,
  a.total_products,
  a.bestseller_count,
  round(a.bestseller_ratio, 4) as bestseller_ratio,
  a.total_reviews,
  round(a.avg_reviews, 2) as avg_reviews,
  a.sum_price,
  round(a.avg_price, 2) as avg_price,
  round(a.avg_stars, 2) as avg_stars,

  -- X축 점수
  round(((a.bestseller_ratio - ag.avg_bsr) / ag.std_bsr) +
        ((a.avg_reviews - ag.avg_reviews) / ag.std_reviews), 3) as X_score,

  -- Y축 점수
  round(((a.sum_price - ag.avg_sum_price) / ag.std_sum_price) +
        ((a.avg_stars - ag.avg_avg_stars) / ag.std_avg_stars), 3) as Y_score

from category_base a
cross join agg_stats ag
order by a.category_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - ### number of reviews vs. star rating (correlation analysis)

-- COMMAND ----------

-- 전체 상품
select
  (
    sum((reviews - stats.avg_reviews) * (stars - stats.avg_stars)) /
    (sqrt(sum(pow(reviews - stats.avg_reviews, 2))) * sqrt(sum(pow(stars - stats.avg_stars, 2))))
  ) as correlation_reviews_stars
from amazon_products_ss, (
  select
    avg(reviews) as avg_reviews,
    avg(stars) as avg_stars
  from amazon_products_ss
  where reviews is not null and stars is not null
) as stats
where reviews is not null and stars is not null;
 


-- 카테고리별
select
  c.category_type,
  count(*) as product_count,
  (
    sum((p.reviews - stats.avg_reviews) * (p.stars - stats.avg_stars)) /
    (sqrt(sum(pow(p.reviews - stats.avg_reviews, 2))) * sqrt(sum(pow(p.stars - stats.avg_stars, 2))))
  ) as correlation_reviews_stars
from amazon_products_ss p
join amazon_categories_with_type c on p.category_id = c.id
join (
  select 
    category_id,
    avg(reviews) as avg_reviews,
    avg(stars) as avg_stars
  from amazon_products_ss
  where reviews > 0 and stars > 0
  group by category_id
) as stats on p.category_id = stats.category_id
where p.reviews is not null and p.stars is not null
group by c.category_type
order by correlation_reviews_stars desc;