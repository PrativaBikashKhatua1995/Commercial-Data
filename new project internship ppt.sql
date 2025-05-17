-- quantity by region and their percentage
SELECT 
    Region,
    SUM(Quantity) AS Total_Quantity,
    ROUND(SUM(Quantity) * 100.0 / SUM(SUM(Quantity)) OVER (), 2) AS Percentage_Contribution
FROM 
    Finalised_Records_1
GROUP BY 
    Region
ORDER BY 
    Total_Quantity DESC;

	--payment method per region	 and percentage
	SELECT 
    Region,
    COUNT(Quantity) AS Payment_Count
	from Finalised_Records_1
	group by Region

	--quantity by payment method percentage 
 	SELECT 
    p.payment_type,
    ROUND(SUM(f.Quantity) * 100.0 / SUM(SUM(f.Quantity)) OVER (), 2) AS percentage_share
FROM 
    Finalised_Records_1 f
JOIN 
    orderpayments1 p ON f.order_id = p.order_id
GROUP BY 
    p.payment_type
ORDER BY 
    percentage_share DESC;

-------------------------quantitity by state and store
select top 5 customer_state,count(Quantity) from Finalised_Records_1
group by customer_state
order by  count(Quantity) desc

select top 5 Delivered_StoreID, COUNT(Quantity)  from Finalised_Records_1
group by Delivered_StoreID
order by  COUNT(Quantity) desc

-----------------------------------quantity by category and their percentage

SELECT 
    Category,
    SUM(Quantity) AS Total_Quantity,
    ROUND(SUM(Quantity) * 100.0 / SUM(SUM(Quantity)) OVER (), 2) AS Percentage_Contribution
FROM Finalised_Records_1
GROUP BY Category
ORDER BY Total_Quantity DESC;

-----------------revenue percentage and quantity by channel 

SELECT Channel, 
    COUNT(Quantity) AS Number_of_Orders, 
    round((SUM([Total Amount])*100)/sum(SUM([Total Amount]))over(),2) AS Revenue_per
FROM Finalised_Records_1
GROUP BY Channel
order by Revenue_per desc;

 -------------- top 5 popular category and their quantity count by each region
WITH cte AS (
    SELECT 
        Region,
        Category,
        SUM(Quantity) AS TotalQuantity,
        ROW_NUMBER() OVER (PARTITION BY Region ORDER BY SUM(Quantity) DESC) AS r
    FROM Finalised_Records_1
    GROUP BY Region, Category
)
SELECT 
    Region,
    Category,
    TotalQuantity,
    r
FROM cte
WHERE r <= 5
ORDER BY Region, r;

-------------------------------------------------top 5 sate with popular category

WITH cte AS (
    SELECT 
        Region,
        Category,
        SUM(Quantity) AS TotalQuantity,
        ROW_NUMBER() OVER (PARTITION BY Region ORDER BY SUM(Quantity) DESC) AS r
    FROM Finalised_Records_1
    GROUP BY Region, Category
)
SELECT 
    Region,
    Category,
    TotalQuantity,
    r
FROM cte
WHERE r <= 5
ORDER BY Region, r;


 -------------------------------- top 5 popular category and their quantity count by each region
WITH cte AS (
    SELECT  
        customer_state,
        SUM(Quantity) AS TotalQuantity,
        ROW_NUMBER() OVER (PARTITION BY Region ORDER BY SUM(Quantity) DESC) AS r
    FROM Finalised_Records_1
    GROUP BY Region, customer_state
)
SELECT 
    customer_state,
    TotalQuantity,
    r
FROM cte
WHERE r <= 5
ORDER BY  r;

--expensive product and their contribution
select top 10 product_id,sum([Total Amount]),(sum([Total Amount])*100)/sum(sum([Total Amount]))over() AS ContributionPercent from Finalised_Records_1
group by product_id
order by sum([Total Amount]) desc

-- top 10 expensive product and sales amount
 select top 10 product_id,sum([Total Amount]) from Finalised_Records_1
 group by product_id
order by sum([Total Amount]) desc

--top 10 profit making product and generating profit
select top 10 f.product_id,sum(o.Total_profit) from Finalised_Records_1  f join _Orders360_ o
on f.order_id=o.Orderid
group by f.product_id
order by sum(o.Total_profit) desc

-- top 10 worst performing stores based on revenue
select top 10 Delivered_StoreID,sum([Total Amount]) from Finalised_Records_1
group by Delivered_StoreID
order by sum([Total Amount]) asc

	select * from customer_360
	select * from _Orders360_
	select * from _store360_
	select * from Finalised_Records_1

---- customer behavior  -------
select top 5 Delivered_StoreID,count(Customer_id) from Finalised_Records_1
group by Delivered_StoreID
order by count(Customer_id) desc

-- cust prefered channel
select channel,count(customer_id) from Finalised_Records_1
group by channel
order by count(customer_id)desc

--customer prefered payment type
select o.payment_type,count(f.Customer_id) from orderpayments1 o	join Finalised_Records_1 f
on o.order_id=f.order_id
group by payment_type
order by  count(f.Customer_id) desc 

--top 5 category by cust count

select top 5 Category,count(Customer_id) from Finalised_Records_1
group by Category
order by  count(Customer_id) desc

--single category buyer and multi category customer  from Finalised_Records_1
 WITH category_counts AS (
    SELECT 
        Customer_id,
        COUNT(DISTINCT Category) AS category_count
    FROM Finalised_Records_1
    GROUP BY Customer_id
),
single_category_customers AS (
    SELECT Customer_id
    FROM category_counts
    WHERE category_count = 1
),
multi_category_customers AS (
    SELECT Customer_id
    FROM category_counts
    WHERE category_count > 1
)
SELECT 
    (SELECT COUNT(*) FROM single_category_customers) AS SingleCategoryBuyers,
    (SELECT COUNT(*) FROM multi_category_customers) AS MultiCategoryBuyers;

--avg categories per order
   with cte as (
   select order_id,count(distinct Category)b    from Finalised_Records_1
   group by order_id )
   select cast(avg(b*1.0) as float) from cte


-- payment_type calculation per region	and their percentagae
 with cte as(
SELECT 
    f.Region as b,
    o.Payment_Type as c,
    COUNT(*) AS PaymentTypeCount
FROM Finalised_Records_1 f
JOIN orderpayments1 o ON f.Order_ID = o.Order_ID
GROUP BY f.Region, o.Payment_Type)
select b,c,paymenttypecount,
	ROUND((PaymentTypeCount * 100.0) / SUM(PaymentTypeCount) OVER (PARTITION BY b), 2) AS Percentage 
	from cte
	ORDER BY  b,PaymentTypeCount desc;

-- max purchase done by which channel
with cte as (
select Channel,count(*) v 
from Finalised_Records_1
group by Channel

) 
select Channel,
v AS ChannelCount,
    ROUND((v * 100.0) / SUM(v) OVER (), 2) AS PercentageShare
	from cte
order by PercentageShare desc

WITH cte AS (
    SELECT  
        COUNT(*) AS v
    FROM Finalised_Records_1
    GROUP BY Channel
)
SELECT 
    Channel,
    v AS ChannelCount,
    ROUND((v * 100.0) / SUM(v) OVER (), 2) AS PercentageShare
FROM cte
ORDER BY PercentageShare DESC;


--- find cust percentage who prefer single category	 vs multi category
 WITH category_count_per_customer AS (
    SELECT 
        Customer_ID,
        COUNT(DISTINCT Category) AS category_count
    FROM Finalised_Records_1
    GROUP BY Customer_ID
),
single_category_customers AS (
    SELECT Customer_ID FROM category_count_per_customer
    WHERE category_count = 1
),
multi_category_customers AS (
    SELECT Customer_ID FROM category_count_per_customer
    WHERE category_count > 1
),
totals AS (
    SELECT 
        (SELECT COUNT(*) FROM single_category_customers) AS single_count,
        (SELECT COUNT(*) FROM multi_category_customers) AS multi_count
)
SELECT 
    single_count,
    multi_count,
    ROUND((single_count * 100.0) / (single_count + multi_count), 2) AS single_percentage,
    ROUND((multi_count * 100.0) / (single_count + multi_count), 2) AS multi_percentage
FROM totals;


 -- weekday and weekend sale female and male count and percentage
WITH male_data AS (
    SELECT 
    COUNT(*) AS male_count,
    SUM(Weekday_Transactions) AS male_weekday_sales,
    SUM(Weekend_Transactions) AS male_weekend_sales
    FROM customer_360
    WHERE Gender = 'm'
),
female_data AS (
    SELECT 
    COUNT(*) AS female_count,
    SUM(Weekday_Transactions) AS female_weekday_sales,
    SUM(Weekend_Transactions) AS female_weekend_sales
    FROM customer_360
    WHERE Gender = 'f'
)
SELECT 
    m.male_count,
    m.male_weekday_sales,
    m.male_weekend_sales,
    ROUND((m.male_weekday_sales * 100.0) / NULLIF((m.male_weekday_sales + m.male_weekend_sales), 0), 2) AS male_weekday_pct,
    ROUND((m.male_weekend_sales * 100.0) / NULLIF((m.male_weekday_sales + m.male_weekend_sales), 0), 2) AS male_weekend_pct,

    f.female_count,
    f.female_weekday_sales,
    f.female_weekend_sales,
    ROUND((f.female_weekday_sales * 100.0) / NULLIF((f.female_weekday_sales + f.female_weekend_sales), 0), 2) AS female_weekday_pct,
    ROUND((f.female_weekend_sales * 100.0) / NULLIF((f.female_weekday_sales + f.female_weekend_sales), 0), 2) AS female_weekend_pct

FROM male_data m
CROSS JOIN female_data f;

 -- top prefered channel
 select distinct channel from Finalised_Records_1

 --month wise top category sales trend seasonality

 WITH monthly_category_sales AS (
    SELECT 
        DATEPART(YEAR, Bill_date_timestamp) AS year,
        DATEPART(MONTH, Bill_date_timestamp) AS month,
        DATENAME(MONTH, Bill_date_timestamp) AS month_name,
        Category,
        SUM([Total Amount]) AS total_sales,
        ROW_NUMBER() OVER (
            PARTITION BY DATEPART(YEAR, Bill_date_timestamp), DATEPART(MONTH, Bill_date_timestamp)
            ORDER BY SUM([Total Amount]) DESC
        ) AS rnk
    FROM Finalised_Records_1
    GROUP BY 
        DATEPART(YEAR, Bill_date_timestamp),
        DATEPART(MONTH, Bill_date_timestamp),
        DATENAME(MONTH, Bill_date_timestamp),
        Category
)
SELECT 
    year,
    month,
    month_name,
    Category,
    total_sales
FROM monthly_category_sales
WHERE rnk = 1
;

--- channel wise profit in percentage
with cte as(
select Channel_used,sum(Total_profit) x  from _Orders360_
group by Channel_used
)
select channel_used,x,
ROUND((x * 100.0) / SUM(x) OVER (), 2) AS profit_percentage from cte
order  by channel_used,x

-- quantity percentage by city
WITH city_quantity AS (
    SELECT 
        seller_city, 
        SUM(Quantity) AS total_quantity
    FROM Finalised_Records_1
    GROUP BY seller_city
),
top_10_cities AS (
    SELECT TOP 10 *
    FROM city_quantity
    ORDER BY total_quantity DESC
),
final AS (
    SELECT 
        seller_city,
        total_quantity,
        SUM(total_quantity) OVER () AS grand_total
    FROM top_10_cities
)
SELECT 
    seller_city,
    total_quantity,
    ROUND((total_quantity * 100.0) / grand_total, 2) AS quantity_percentage
FROM final
ORDER BY total_quantity DESC;

--channel with less demand in oredr
select channel,sum([Total Amount]) from Finalised_Records_1
group by Channel
order by  sum([Total Amount]) asc





