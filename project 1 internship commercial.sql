  -- #######################   questions with answers ###############################################
select count(Bill_date_timestamp) from Finalised_Records_1
where Bill_date_timestamp not in( select Bill_date_timestamp from Finalised_Records_1
where Bill_date_timestamp > '01-09-2021' and Bill_date_timestamp< '13-10-2023')

select * from customer_360
--The number of orders,
select count( orderid) as numbrs from _Orders360_;

--total quantity
select sum(Quantity) from Finalised_Records_1

--total cost
select sum([Cost Per Unit]) from Finalised_Records_1

--Total Discount, 
select sum(discount) as total from Finalised_Records_1;

--Average discount per customer, 
select round(avg(Discount),2) as avrg from Finalised_Records_1;

--Average discount per order,
select round(AVG(discount),2) as average_discount
from Finalised_Records_1

--Average order value or Average Bill Value,
select round(avg([Total Amount]),2) as average_bill from Finalised_Records_1;

-- no of customers with total revenue
select count(Customer_id) no_of_cust,round(sum([Total Amount]),2) rev from Finalised_Records_1

--Average Sales per Customer,
select  round(sum([Total Amount])/count(distinct customer_id),2) as average_rate
from Finalised_Records_1 

--Average profit per customer, 
select round(AVG( [Total Amount]- [Cost Per Unit] * quantity),2)  as average_rate
 from  Finalised_Records_1 

--average number of categories per order,
 

select * from Finalised_Records_1
--average number of items per order,
select  round(avg(Quantity),2) as avrg from Finalised_Records_1

--Number of customers,
select count(distinct Customer_id) from Finalised_Records_1;

--Transactions per Customer, 
select  round(sum([Total Amount])/count(Customer_id),2) from Finalised_Records_1

--Total Revenue, 
select round(sum([Total Amount]),2) as revenue from Finalised_Records_1;



--Total Cost,
select SUM([Total Amount]) as total from Finalised_Records_1;

--Total quantity,
select sum(Quantity) as total_quantity from Finalised_Records_1

--Total products,
select count(distinct product_id) from Finalised_Records_1

--Total categories,
select count(distinct Category) as category from Finalised_Records_1

--Total stores, 
select count (distinct Delivered_StoreID) from Finalised_Records_1;

--Total locations,
select count (distinct customer_city) from Finalised_Records_1

--Total Regions,
select count (distinct Region) from Finalised_Records_1

--Total channels,
select count (distinct channel) from Finalised_Records_1

--Total payment methods,
select count (distinct payment_type) from orderpayments1

--Average minutes between two transactions (if the customer has more than one transaction), 
WITH ordered_transactions AS (
    SELECT 
        Customer_ID,
        Bill_date_timestamp,
        LAG(Bill_date_timestamp) OVER (PARTITION BY Customer_ID ORDER BY Bill_date_timestamp) AS prev_transaction
    FROM Finalised_Records_1
),
time_diffs AS (
    SELECT 
        Customer_ID,
        DATEDIFF(MINUTE, prev_transaction, Bill_date_timestamp) AS diff_minutes
    FROM ordered_transactions
    WHERE prev_transaction IS NOT NULL
)
SELECT 
    AVG(diff_minutes * 1.0) AS avg_minutes_between_transactions
FROM time_diffs;

--profit making region
select f.region,round(sum(o.Total_profit),2) from _Orders360_ o  join Finalised_Records_1 f
on o.Orderid=f.order_id
group by f.region
order by f.region desc


	select * from _Orders360_
--percentage of profit,
select round(sum(mrp*Quantity -[Total Amount])/sum(mrp*Quantity)*100,2) as total_profit 
from Finalised_Records_1

--percentage of discount,
select round(sum(Discount)/sum([Total Amount])*100,2) as discount 
from Finalised_Records_1;

--Repeat customer percentage, 
with cte as (select customer_id,month(Bill_date_timestamp) month,count(month(Bill_date_timestamp)) repetation
from Finalised_Records_1
group by Customer_id ,month(Bill_date_timestamp)
having count(month(Bill_date_timestamp))>1)
select round(cast(count(distinct cte.Customer_id) as float)/count(distinct f.customer_id)*100,2) as percentage from Finalised_Records_1 f
left join cte on cte.Customer_id=f.Customer_id


--One time buyers percentage etc…
with cte as (SELECT 
    Customer_id as count,
    MONTH(Bill_date_timestamp) AS purchase_month
FROM Finalised_Records_1 a
GROUP BY Customer_id, MONTH(a.Bill_date_timestamp)
HAVING COUNT(*) = 1)
select count(count)*1.0/(select COUNT(customer_id) from Finalised_Records_1)*100 from cte



--avg product per customer
select round(count(distinct product_id) * 1.0 /count(distinct Customer_id) ,2)
from Finalised_Records_1


--avg product rating per customer
with cte as (select product_id,avg(Avg_rating) as num
from Finalised_Records_1
group by product_id)
select round(avg(num*1.0),2) from cte

--avg product sold and customer from region and their percentage
WITH cte AS (
    SELECT 
        Region,
        COUNT(DISTINCT Customer_id) AS count_cust,
        COUNT(product_id) AS count_pro
    FROM Finalised_Records_1
    GROUP BY Region
),
totals AS (
    SELECT 
        SUM(count_cust) AS total_cust,
        SUM(count_pro) AS total_pro
    FROM cte
)
SELECT 
    cte.Region,
    cte.count_cust AS total_customers_in_region,
    cte.count_pro AS total_products_in_region,
    ROUND((cte.count_cust * 100.0) / t.total_cust, 2) AS customer_percentage,
    ROUND((cte.count_pro * 100.0) / t.total_pro, 2) AS product_percentage
FROM cte
CROSS JOIN totals t
ORDER BY customer_percentage DESC;

-- profit making region and  therir percentage
WITH RegionProfit AS (
    SELECT 
        f.Region,
        SUM(o.Total_profit) AS total_profit_by_region
    FROM _Orders360_ o 
    JOIN Finalised_Records_1 f ON o.Orderid = f.order_id
    GROUP BY f.Region
),
TotalProfit AS (
    SELECT SUM(total_profit_by_region) AS overall_profit
    FROM RegionProfit
)
SELECT 
    rp.Region,
    rp.total_profit_by_region,
    ROUND((rp.total_profit_by_region * 100.0) / tp.overall_profit, 2) AS profit_percentage
FROM RegionProfit rp
CROSS JOIN TotalProfit tp
ORDER BY rp.total_profit_by_region DESC;



--highest profit making store
select top 1 Delivered_StoreID,Region from Finalised_Records_1 
group by Delivered_StoreID,Region
order by sum(mrp*Quantity -Discount) desc

select * from Finalised_Records_1

--Understanding how many new customers acquired every month (who made transaction first time in the data)
WITH cte AS (
    SELECT 
        customer_id,
        MIN(bill_date_timestamp) AS first_purchase_date
    FROM 
        Finalised_Records_1
    GROUP BY 
        customer_id
)
SELECT 
    FORMAT(first_purchase_date, 'yyyy-MM') AS purchase_month,
    COUNT(customer_id) AS new_customers, 
    COUNT(customer_id) * 100.0 / (SELECT COUNT(*) FROM cte) AS percenti -- Percentage calculation
FROM 
    cte
GROUP BY 
    FORMAT(first_purchase_date, 'yyyy-MM')
ORDER BY 
    new_customers ASC;



-- count of product id by region
select region,count(product_id)from Finalised_Records_1
group by Region

select * from Finalised_Records_1
--total mrp
select sum(MRP) from Finalised_Records_1

--Understand the retention of customers on month on month basis 
with cte as (
SELECT  year(Bill_date_timestamp) y,MONTH(Bill_date_timestamp) AS m	,count(customer_id) as numbers
FROM Finalised_Records_1
 group by  year(Bill_date_timestamp),MONTH(Bill_date_timestamp)
 having count(Customer_id) >1
 )
 select  y,m,numbers, (numbers * 100.0) / SUM(numbers) OVER () AS percentage from cte
 order by y,numbers desc

--How the revenues from existing/new customers on monthly basis
-- Step 1: Identify the first purchase date per customer
WITH FirstPurchase AS (
    SELECT 
        customer_id,
        MIN(Bill_date_timestamp) AS FirstPurchaseDate
    FROM Finalised_Records_1
    GROUP BY customer_id
),

-- Step 2: Classify each transaction as 'New' or 'Existing' customer
CustomerStatus AS (
    SELECT 
        f.*,
        CASE 
            WHEN CAST(f.Bill_date_timestamp AS DATE) = CAST(fp.FirstPurchaseDate AS DATE) THEN 'New'
            ELSE 'Existing'
        END AS Customer_Type
    FROM Finalised_Records_1 f
    JOIN FirstPurchase fp 
        ON f.customer_id = fp.customer_id
),

-- Step 3: Aggregate monthly revenue by customer type
MonthlyRevenue AS (
    SELECT 
        DATEPART(YEAR, Bill_date_timestamp) AS Year,
        DATEPART(MONTH, Bill_date_timestamp) AS Month,
        Customer_Type,
        SUM([Total Amount]) AS Total_Revenue
    FROM CustomerStatus
    GROUP BY 
        DATEPART(YEAR, Bill_date_timestamp),
        DATEPART(MONTH, Bill_date_timestamp),
        Customer_Type
)

-- Step 4: Pivot for comparison
SELECT 
    Year,
    Month,
    ISNULL(SUM(CASE WHEN Customer_Type = 'New' THEN Total_Revenue END), 0) AS New_Customer_Revenue,
    ISNULL(SUM(CASE WHEN Customer_Type = 'Existing' THEN Total_Revenue END), 0) AS Existing_Customer_Revenue
FROM MonthlyRevenue
GROUP BY Year, Month
ORDER BY Year, Month;







-- Step 1: Find the first purchase date of each customer

WITH CustomerFirstPurchase AS (
    SELECT 
        Customer_id,
        MIN(CAST(bill_date_timestamp AS DATE)) AS first_purchase_date
    FROM 
        Finalised_Records_1
    GROUP BY 
        Customer_id
),
-- Step 2: Tag each order whether customer is 'New' or 'Existing' based on month
OrdersWithCustomerType AS (
    SELECT 
        f.Customer_id,
        f.[Total Amount],
        FORMAT(f.bill_date_timestamp, 'yyyy-MM') AS order_month,
        CASE 
            WHEN FORMAT(f.bill_date_timestamp, 'yyyy-MM') = FORMAT(cfp.first_purchase_date, 'yyyy-MM')
            THEN 'New'
            ELSE 'Existing'
        END AS customer_type
    FROM 
        Finalised_Records_1 f
    INNER JOIN 
        CustomerFirstPurchase cfp
    ON 
        f.Customer_id = cfp.Customer_id
)
-- Step 3: Aggregate Revenue by Month and Customer Type
SELECT 
    order_month,
    customer_type,
    SUM([Total Amount]) AS total_revenue
FROM 
    OrdersWithCustomerType
GROUP BY 
    order_month,
    customer_type
ORDER BY 
    order_month, customer_type;



--Understand the trends/seasonality of sales, quantity by category, region, store, channel, payment method etc…
SELECT TOP 5  
    f.Category,
    f.Bill_date_timestamp,
    f.Quantity,
    f.Region,
    f.Delivered_StoreID,
    f.Channel,
    o.payment_type,
    f.[Total Amount]
FROM Finalised_Records_1 f 
JOIN orderpayments1 o ON f.order_id = o.order_id
ORDER BY f.[Total Amount] DESC;

--Popular categories/Popular Products by store, state, region.
SELECT TOP 5 
    Category,
    product_id,
    seller_state,
    Region,
    Delivered_StoreID,
    SUM(Quantity) AS Total_Sold
FROM Finalised_Records_1
GROUP BY 
    Category,
    product_id,
    seller_state,
    Region,
    Delivered_StoreID
ORDER BY Total_Sold DESC;


----------------List the top 10 most expensive products sorted by price and their contribution to sales

WITH cte AS (
    SELECT TOP 10 
        product_id,
        region,
        SUM([Total Amount]) AS amount
    FROM Finalised_Records_1
    GROUP BY product_id, region
    ORDER BY amount DESC  -- Important: sort by highest amount
),
total_sales AS (
    SELECT SUM([Total Amount]) AS total_amount
    FROM Finalised_Records_1
)
SELECT 
    cte.product_id,
    cte.region,
    cte.amount,
    (cte.amount * 100.0 / total_sales.total_amount) AS contribution_percentage
FROM 
    cte
CROSS JOIN 
    total_sales
ORDER BY 
    cte.amount DESC;


--------------Which products appeared in the transactions?--
SELECT 
    product_id,
    MIN(Bill_date_timestamp) AS First_Product_Transaction
FROM Finalised_Records_1
WHERE product_id IN (SELECT product_id FROM ProductsInfo)
GROUP BY product_id;



------------------Top best 10 best performing & worst 10 performance stores in terms of sales

    SELECT TOP 10 
        Delivered_StoreID,
        SUM([Total Amount]) AS total_amount
    FROM Finalised_Records_1
    GROUP BY Delivered_StoreID
    ORDER BY total_amount DESC
--
	SELECT TOP 10 
        Delivered_StoreID,
        SUM([Total Amount]) AS total_amount
    FROM Finalised_Records_1
    GROUP BY Delivered_StoreID
    ORDER BY total_amount asc

------------------2. Customer Behaviour
--Segment the customers (divide the customers into groups) based on the revenue
	
 WITH CustomerRevenue AS (
 SELECT Customer_id, SUM([Total Amount]) AS TotalRevenue
 FROM Finalised_Records_1
 GROUP BY Customer_id),
RankedCustomers AS (
 SELECT  *, NTILE(3) OVER (ORDER BY TotalRevenue DESC) AS RevenueSegment
 FROM CustomerRevenue)
 SELECT 
 CASE 
    WHEN RevenueSegment = 1 THEN 'High Revenue Customers'
    WHEN RevenueSegment = 2 THEN 'Medium Revenue Customers'
    WHEN RevenueSegment = 3 THEN 'Low Revenue Customers'
 END AS Segment,
 COUNT(*) AS Customer_Count
 FROM RankedCustomers
 GROUP BY RevenueSegment
 ORDER BY RevenueSegment;

---------------------Divide the customers into groups based on Recency, Frequency, and Monetary (RFM Segmentation) -  Divide the customers into Premium,
--Gold, Silver, Standard customers and understand the behaviour of each segment of customers


WITH expenditure_ranks AS (
    SELECT *,
           NTILE(3) OVER (ORDER BY Total_expenditure DESC) AS rank_segment
    FROM customer_360
)
SELECT *,
       CASE 
           WHEN rank_segment = 1 THEN 'PLATINUM'
          
           WHEN rank_segment = 3 THEN 'SILVER'
           ELSE 'LOW VALUE'
       END AS CUSTOMER_SEGMENTATION
FROM expenditure_ranks;


-------------------------Find out the number of customers who purchased in all the channels and find the key metrics.
-- Step 1: Get the total number of distinct channels
WITH total_channels AS (
    SELECT COUNT(DISTINCT Channel) AS total_channel_count
    FROM Finalised_Records_1
),

-- Step 2: For each customer, count the number of distinct channels they purchased from
customer_channel_count AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT Channel) AS customer_channel_count
    FROM Finalised_Records_1
    GROUP BY customer_id
)

-- Step 3: Find customers whose channel count = total channel count
SELECT 
    c.customer_id
FROM 
    customer_channel_count c
CROSS JOIN 
    total_channels t
WHERE 
    c.customer_channel_count = t.total_channel_count;

--------------------Understand the behavior of one time buyers and repeat buyers ---
  WITH one_time_buyers AS (
    SELECT customer_id
    FROM Finalised_Records_1
    GROUP BY customer_id
    HAVING COUNT(*) = 1
),
repeat_buyers AS (
    SELECT customer_id
    FROM Finalised_Records_1
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
SELECT 
    (SELECT COUNT(*) FROM one_time_buyers) AS one_time_buyer_count,
    (SELECT COUNT(*) FROM repeat_buyers) AS repeat_buyer_count;






-- Step 1: finding total number of distinct channels
WITH total_channels AS (
    SELECT COUNT(DISTINCT Channel) AS total_channel_count
    FROM Finalised_Records_1
),

-- Step 2: then for each customer, count the number of distinct channels they purchased from
customer_channel_count AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT Channel) AS customer_channel_count
    FROM Finalised_Records_1
    GROUP BY customer_id
)

-- Step 3: Find customers whose channel count = total channel count
SELECT 
    c.customer_id
FROM 
    customer_channel_count c
CROSS JOIN 
    total_channels t
WHERE 
    c.customer_channel_count = t.total_channel_count;



--Understand the behavior of discount seekers & non discount seekers
WITH CustomerDiscounts AS (
    SELECT 
        Customer_id,
        Category,
        SUM(Discount) AS total_discount
    FROM 
        Finalised_Records_1
    GROUP BY 
        Customer_id, Category
), 
cte2 AS (
    SELECT 
        Customer_id,
        Category,
        total_discount,
        CASE 
            WHEN total_discount > 00 THEN 'Discount Seeker'
            ELSE 'Non-Discount Seeker'
        END AS discount_behavior
    FROM 
        CustomerDiscounts
)
-- Final aggregation
SELECT 
    discount_behavior,
    COUNT(DISTINCT Customer_id) AS total_customers
FROM 
    cte2
GROUP BY 
    discount_behavior;

 ---------------------------------- some more insights like female or male count who are prefering discounts 
 WITH CustomerDiscounts AS (
    SELECT 
        Customer_id,
        SUM(Discount) AS total_discount
    FROM Finalised_Records_1
    GROUP BY Customer_id
), 
DiscountBehavior AS (
    SELECT 
        cd.Customer_id,
        CASE 
            WHEN cd.total_discount > 0 THEN 'Discount Seeker'
            ELSE 'Non-Discount Seeker'
        END AS discount_behavior
    FROM CustomerDiscounts cd
),
GenderInfo AS (
    SELECT DISTINCT Customer_id, Gender
    FROM Finalised_Records_1
),
Combined AS (
    SELECT 
        db.discount_behavior,
        gi.Gender
    FROM DiscountBehavior db
    JOIN GenderInfo gi ON db.Customer_id = gi.Customer_id
),
GenderCounts AS (
    SELECT 
        discount_behavior,
        Gender,
        COUNT(*) AS gender_count
    FROM Combined
    GROUP BY discount_behavior, Gender
),
TotalPerGroup AS (
    SELECT 
        discount_behavior,
        SUM(gender_count) AS total_count
    FROM GenderCounts
    GROUP BY discount_behavior
)
SELECT 
    gc.discount_behavior,
    gc.Gender,
    gc.gender_count,
    tp.total_count,
    ROUND(100.0 * gc.gender_count / NULLIF(tp.total_count, 0), 2) AS gender_percentage
FROM GenderCounts gc
JOIN TotalPerGroup tp ON gc.discount_behavior = tp.discount_behavior
ORDER BY gc.discount_behavior, gc.Gender;





--Understand preferences of customers (preferred channel, Preferred payment method, preferred store, discount preference, preferred categories etc.)

select top 10 Customer_id,sum(Channels_used) channel, sum(Credit_Payments)credit,sum(Debit_Payments)debit,
sum(UPI_CASH_Payments)upi,sum(Instore_Transactions)instore 
from customer_360
group by Customer_id
order by credit desc


  select * from customer_360
--Understand the behavior of customers who purchased one category and purchased multiple categories**********************************
WITH single_category_customers AS (
    SELECT 
        Customer_id
    FROM Finalised_Records_1
    GROUP BY Customer_id
    HAVING COUNT(DISTINCT Category) = 1
),
multiple_category_customers AS (
    SELECT 
        Customer_id
    FROM Finalised_Records_1
    GROUP BY Customer_id
    HAVING COUNT(DISTINCT Category) > 1
)
SELECT 
    (SELECT COUNT(*) FROM single_category_customers) AS single_category_customer_count,
    (SELECT COUNT(*) FROM multiple_category_customers) AS multiple_category_customer_count;



--3. Cross-Selling (Which products are selling together) 
--Hint: We need to find which of the top 10 combinations of products are selling together in each transaction..
--(combination of 2 or 3 buying together) 
	   select top 10 f1.Category,f2.Category
	   from Finalised_Records_1 f1  join Finalised_Records_1 f2
	   on f1.Customer_id=f2.Customer_id and f1.product_id<>f2.product_id  and f1.category<>f2.category
	   join customer_360 c on f1.Customer_id=c.Customer_id
	   where c.First_Transaction_Date in(select First_Transaction_Date from customer_360)

	   -- Step 1: Get top 5 combinations of different categories bought together each month
SELECT  concat (datepart(year,f1.Bill_date_timestamp),'-', datepart(month,f1.Bill_date_timestamp)) ,
    f1.Category AS Category1,
    f2.Category AS Category2,
    COUNT(DISTINCT f1.Customer_id  ) AS Num_Transactions
FROM Finalised_Records_1 f1
JOIN Finalised_Records_1 f2 
    ON f1.Customer_id = f2.Customer_id
    AND f1.Bill_date_timestamp = f2.Bill_date_timestamp  
    AND f1.Product_id < f2.Product_id             
    AND f1.Category <> f2.Category  
GROUP BY datepart(year,f1.Bill_date_timestamp), datepart(month,f1.Bill_date_timestamp),f1.Category, f2.Category,f1.Bill_date_timestamp 
ORDER BY Num_Transactions DESC;


	
--4. Understand the Category Behavior
--Total Sales & Percentage of sales by category (Perform Pareto Analysis)
 WITH Total_CategSales AS (
    SELECT 
        Category,
        ROUND(SUM([Total Amount]), 2) AS total_sales
    FROM Finalised_Records_1
    GROUP BY Category
),
Cumm_Sales AS (
    SELECT 
        *,
        SUM(total_sales) OVER (ORDER BY total_sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cummutative_sales
    FROM Total_CategSales
),
Cumm_per As(
Select *,ROUND(cummutative_sales * 1.0 / SUM(total_sales) OVER ()*100, 4) as cummutative_per from Cumm_Sales
)
SELECT 
    Category,
    ROUND(total_sales / 1000000.0, 2) AS Sales_Million,
    ROUND(cummutative_sales / 1000000.0, 2) AS Cumulative_Sales_Million,
    CONCAT(ROUND(cummutative_per, 0), '%') AS [CUM% of Sales]
FROM Cumm_per;



--Most profitable category and its contribution  

WITH cte AS (
    SELECT  
        f.category,
        SUM(f.[Total Amount]) AS total_revenue
    FROM Finalised_Records_1 o
    JOIN Finalised_Records_1 f ON f.order_id = f.order_id
    GROUP BY f.category
),
max_cte AS (
    SELECT TOP 1 
        category,
        total_revenue
    FROM cte
    ORDER BY total_revenue DESC
),
total_cte AS (
    SELECT 
        SUM(total_revenue) AS total_revenue_all
    FROM cte
)
SELECT 
    max_cte.category,
    max_cte.total_revenue,
    ROUND(100.0 * max_cte.total_revenue / NULLIF(total_cte.total_revenue_all, 0), 2) AS percentage_contribution
FROM max_cte
CROSS JOIN total_cte;
 
  
--Category Penetration Analysis by month on month (Category Penetration = number of orders containing the category/number of orders)
=/*Cross Category Analysis by month on Month (In Every Bill, how many categories shopped. Need to calculate average number of categories shopped in each bill by Region,
By State etc)*/
WITH cte AS (
    SELECT 
        YEAR(Bill_date_timestamp) AS year_num,
        MONTH(Bill_date_timestamp) AS month_num,
        SUM([Total Amount]) AS rev_growth
    FROM Finalised_Records_1
    GROUP BY YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp)
),
cte2 AS (
    SELECT 
        year_num,
        month_num,
        rev_growth,
        LAG(rev_growth, 1) OVER (ORDER BY year_num, month_num) AS prev_month_revenue
    FROM cte
)
SELECT 
    year_num,
    month_num,
    rev_growth,
    prev_month_revenue,
    CASE 
        WHEN prev_month_revenue IS NULL THEN NULL
        ELSE ROUND(((rev_growth - prev_month_revenue) * 100.0 / prev_month_revenue), 2)
    END AS mom_growth_percentage
FROM cte2
ORDER BY year_num, month_num;

--Most popular category during first purchase of customer
WITH FirstPurchase AS (
    SELECT 
        customer_id,
        MIN(Bill_date_timestamp) AS First_Purchase_Date
    FROM Finalised_Records_1
    GROUP BY customer_id
),
FirstPurchaseRecords AS (
    SELECT 
        f.customer_id,
        f.Category,
        f.Quantity
    FROM Finalised_Records_1 f
    INNER JOIN FirstPurchase fp 
        ON f.customer_id = fp.customer_id
       AND f.Bill_date_timestamp = fp.First_Purchase_Date
)
SELECT TOP 1 
    Category,
    COUNT(*) AS Purchase_Count
FROM FirstPurchaseRecords
GROUP BY Category
ORDER BY Purchase_Count DESC;



--5. Customer satisfaction towards category & product 
select f.Category,f.product_id,avg(f.Avg_rating) avg_satisfaction_towards_category
from _Orders360_   o join Finalised_Records_1 f
on o.Orderid=f.order_id 
group by  f.Category,f.product_id

--Which categories (top 10) are maximum rated & minimum rated and average rating score? 
select top 10 f.Category,cast(max(f.Avg_rating) as float) max_rating,cast(avg(f.Avg_rating)as float) avg_rating, cast(min(Avg_rating)as float) min_rating 
from Finalised_Records_1 f join _Orders360_ o
on f.order_id=o.Orderid
group by  f.Category

--Average rating by location, store, product, category, month, etc.
select top 10 product_id,Category,seller_state,month(Bill_date_timestamp) month,Avg_rating 
from Finalised_Records_1
order by Avg_rating;

--6. Perform cohort analysis (customer retention for month on month and retention for fixed month)
  select * from _Orders360_

WITH fo AS (
    SELECT 
        Customer_ID,
        MIN(DATEFROMPARTS(YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp), 1)) AS cohort_month
    FROM Finalised_Records_1
    GROUP BY Customer_ID
),
orders_by_month AS (
    SELECT 
        Customer_ID,
        DATEFROMPARTS(YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp), 1) AS order_month
    FROM Finalised_Records_1
),
cohort_analysis AS (
    SELECT 
        f.cohort_month,
        o.order_month,
        DATEDIFF(MONTH, f.cohort_month, o.order_month) AS month_offset,
        COUNT(DISTINCT o.Customer_ID) AS retained_customers
    FROM fo f
    JOIN orders_by_month o ON f.Customer_ID = o.Customer_ID
    GROUP BY f.cohort_month, o.order_month
)
SELECT 
    cohort_month,
    month_offset,
    retained_customers
FROM cohort_analysis
ORDER BY cohort_month, month_offset;

*--"Customers who started in each month and understand their behavior in the respective months

 WITH first_orders AS (
    -- Identify first purchase month (cohort) for each customer
    SELECT 
        Customer_ID,
        MIN(DATEFROMPARTS(YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp), 1)) AS cohort_month
    FROM Finalised_Records_1
    GROUP BY Customer_ID
),
orders_by_month AS (
    -- Get every month the customer made a purchase
    SELECT 
        Customer_ID,
        DATEFROMPARTS(YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp), 1) AS order_month
    FROM Finalised_Records_1
),
cohort_behavior AS (
    -- Track behavior by comparing order month to cohort month
    SELECT 
        f.cohort_month,
        o.order_month,
        DATEDIFF(MONTH, f.cohort_month, o.order_month) AS month_offset,
        COUNT(DISTINCT o.Customer_ID) AS active_customers
    FROM first_orders f
    JOIN orders_by_month o ON f.Customer_ID = o.Customer_ID
    GROUP BY f.cohort_month, o.order_month
),
base_counts AS (
    -- Get total customers in each cohort
    SELECT 
        cohort_month,
        COUNT(DISTINCT Customer_ID) AS cohort_size
    FROM first_orders
    GROUP BY cohort_month
)
SELECT 
    cb.cohort_month,
    cb.month_offset,
    cb.active_customers,
    bc.cohort_size,
    ROUND(100.0 * cb.active_customers / NULLIF(bc.cohort_size, 0), 2) AS retention_rate
FROM cohort_behavior cb
JOIN base_counts bc ON cb.cohort_month = bc.cohort_month
ORDER BY cb.cohort_month, cb.month_offset;


*--(Example: If 100 new customers started in Jan -2023, how is the 100 new customer behavior 
--(in terms of purchases, revenue, etc..) in Feb-2023, Mar-2023, Apr-2023, etc...)

WITH first_orders AS (
    -- Get each customer's cohort (first purchase) month
    SELECT 
        Customer_ID,
        MIN(DATEFROMPARTS(YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp), 1)) AS cohort_month
    FROM Finalised_Records_1
    GROUP BY Customer_ID
),
orders_by_month AS (
    -- Get monthly purchases
    SELECT 
        Customer_ID,
        DATEFROMPARTS(YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp), 1) AS order_month,
        SUM([Total Amount]) AS revenue
    FROM Finalised_Records_1
    GROUP BY Customer_ID, DATEFROMPARTS(YEAR(Bill_date_timestamp), MONTH(Bill_date_timestamp), 1)
),
cohort_behavior AS (
    -- Join to track cohort behavior
    SELECT 
        f.cohort_month,
        o.order_month,
        DATEDIFF(MONTH, f.cohort_month, o.order_month) AS month_offset,
        COUNT(DISTINCT o.Customer_ID) AS active_customers,
        SUM(o.revenue) AS total_revenue
    FROM first_orders f
    JOIN orders_by_month o ON f.Customer_ID = o.Customer_ID
    GROUP BY f.cohort_month, o.order_month
),
base_counts AS (
    -- Total customers in each cohort
    SELECT 
        cohort_month,
        COUNT(DISTINCT Customer_ID) AS cohort_size
    FROM first_orders
    GROUP BY cohort_month
)
SELECT 
    cb.cohort_month,
    cb.month_offset,
    cb.active_customers,
    bc.cohort_size,
    ROUND(100.0 * cb.active_customers / NULLIF(bc.cohort_size, 0), 2) AS retention_rate,
    cb.total_revenue
FROM cohort_behavior cb
JOIN base_counts bc ON cb.cohort_month = bc.cohort_month
ORDER BY cb.cohort_month, cb.month_offset;


--Which Month cohort has maximum retention?"
WITH Total_CategSales AS (
    SELECT 
        Category,
        ROUND(SUM([Total Amount]), 2) AS total_sales
    FROM Finalised_Records_1
    GROUP BY Category
),
Cumm_Sales AS (
    SELECT 
        *,
        SUM(total_sales) OVER (ORDER BY total_sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cummutative_sales
    FROM Total_CategSales
),
Cumm_per As(
Select *,ROUND(cummutative_sales * 1.0 / SUM(total_sales) OVER ()*100, 4) as cummutative_per from Cumm_Sales
)
SELECT 
    Category,
    ROUND(total_sales / 1000000.0, 2) AS Sales_Million,
    ROUND(cummutative_sales / 1000000.0, 2) AS Cumulative_Sales_Million,
    CONCAT(ROUND(cummutative_per, 0), '%') AS [CUM% of Sales]
FROM Cumm_per;



--7. Perform analysis related to Sales Trends, patterns, and seasonality.
 select DATEPART(YEAR,Bill_date_timestamp) year,DATEname(MONTH,Bill_date_timestamp) as month ,max(Category) trend ,sum([Total Amount]) as rev
 from Finalised_Records_1
 group by DATEname(MONTH,Bill_date_timestamp),DATEPART(YEAR,Bill_date_timestamp)
 order by  sum([Total Amount])  desc





 select* from Finalised_Records_1
--"Which months have had the highest sales, what is the sales amount and contribution in percentage?
select top 1 month(Bill_date_timestamp) month,sum([Total Amount]) maxm_amount, round(sum([Total Amount])/sum([Total Amount])*100,2) as percentage
from Finalised_Records_1
group by month(Bill_date_timestamp)
order by max([Total Amount]) desc

--Which months have had the least sales, what is the sales amount and contribution in percentage? 

 with cte as (
 select top 1 order_id,month(Bill_date_timestamp) month,count(order_id) order_count
 from Finalised_Records_1
 group by  month(Bill_date_timestamp),order_id
 order by month(Bill_date_timestamp) asc)
 select	 month,order_count,order_count/count(order_id)*100 as percentage_order from cte
 group by  month,order_count

--Sales trend by month ---------------------------------------------------------------
WITH cte AS (
    SELECT 
        FORMAT(bill_date_timestamp, 'yyyy-MM') AS sales_month,  -- Month in YYYY-MM format
        category,
        SUM([Total Amount]) AS total_sales
    FROM 
        Finalised_Records_1
    GROUP BY 
        FORMAT(bill_date_timestamp, 'yyyy-MM'),
        category
)
SELECT 
    sales_month,
    category,
    total_sales
FROM 
    cte
ORDER BY 
    sales_month,
    category;


--Is there any seasonality in the sales (weekdays vs. weekends, months, days of week, weeks etc.)?
SELECT TOP 50 
    f.product_id,
    o.Day_of_week,
    MONTH(f.Bill_date_timestamp) AS month,
    SUM(c.Weekday_Transactions) AS total_weekday_transactions,
    SUM(c.Weekend_Transactions) AS total_weekend_transactions,
    COUNT(*) AS repetition_of_product
FROM 
    Finalised_Records_1 f
INNER JOIN 
    _Orders360_ o ON f.order_id = o.Orderid
INNER JOIN 
    customer_360 c ON c.Customer_id = f.Customer_id
GROUP BY 
    f.product_id,
    o.Day_of_week,
    MONTH(f.Bill_date_timestamp)
HAVING 
    COUNT(*) > 1
ORDER BY 
    repetition_of_product DESC;


 
 

 --revenue --same
  select sum([Total Amount])	 from Finalised_Records_1
  select sum(Total_Amount) from _Orders360_
  --customer id	--98230/96811 may be some duplicates r there 
  select count(Customer_id) from Finalised_Records_1
  select count(Customer_id) from customer_360
  --quantity --same
  select sum(Quantity) from Finalised_Records_1
  select sum(Qty) from _Orders360_
  --productid--98230/98078 nmay be some duplicates r there
  select count(product_id) from Finalised_Records_1
  select count(product_id) from _Orders360_
  --discount --same
  select sum(Discount) from Finalised_Records_1
  select sum(Discount) from _Orders360_
  --order id  98230/98078 little bit diff is there 
  select count(order_id) from Finalised_Records_1
  select count(Orderid) from _Orders360_
  --profit-- nearly 30k gap
  select sum(MRP-discount*Quantity) from Finalised_Records_1
  select avg(Total_profit) from _Orders360_
  -- discout seekers vs non discount seekers
  select sum(Discount),count(Customer_id), count(Customer_id)/sum(Discount)*100 from Finalised_Records_1
  where discount>0


   -- discout seekers vs non discount seekers
 select count(Discount) from Finalised_Records_1
  where Discount= 0
 
 
 select * from customer_360
 select * from 	_Orders360_
 select * from _store360_
 select * from Finalised_Records_1

--Total Sales by Week of the Day, Week, Month, Quarter, Weekdays vs. weekends etc."
select month(f.Bill_date_timestamp)as mnth,o.Day_of_week,c.Weekend_Transactions as weekend,c.Weekday_Transactions as weekday,
datepart(week,f.Bill_date_timestamp) as week,datepart(QUARTER,f.Bill_date_timestamp)as quarter,
sum(f.[Total Amount]) total_sales
from Finalised_Records_1	f join _Orders360_ o
on f.order_id=o.Orderid
join customer_360 c
on f.Customer_id=c.Customer_id
group by month(f.Bill_date_timestamp),o.Day_of_week,c.Weekend_Transactions,c.Weekday_Transactions,datepart(week,f.Bill_date_timestamp),datepart(QUARTER,f.Bill_date_timestamp)


--- no of cities & states
select count(distinct customer_city)customer_city,count(distinct seller_city)seller_city,count(distinct seller_state)seller_state
from Finalised_Records_1

--profit percentage
select round(avg(Total_Profit), 2) as Average_profit_per_customer from _Orders360_;

select sum(Total_Amount)/sum(Total_profit) from _Orders360_  

--discount percentage
 select avg(Total_Discount) from customer_360

-- week revenue
select DATEPART(week,Bill_date_timestamp ) as week, sum([Total Amount]) as rev from Finalised_Records_1
group by DATEPART(week,Bill_date_timestamp )
order by rev desc

-- month revenue
select month(Bill_date_timestamp) month, sum([Total Amount]) total_amount from Finalised_Records_1
group by   month(Bill_date_timestamp) 

--no of stores --37
select count(distinct Delivered_StoreID) from Finalised_Records_1

--distinct channels--3
 select count(distinct Channel) from Finalised_Records_1

--make chart state wise customer distribution
 with cte as (select customer_state,count(customer_state) as numbers
 from Finalised_Records_1
 group by customer_state
 )
 select sum(numbers)/100 from cte
 --- category type
 select  channel,sum([Total Amount]) from Finalised_Records_1
 group by Channel


--lowest revenue generation state and highest
with cte as (select top 1 seller_state, sum([Total Amount]) from Finalised_Records_1
group by seller_state
order by  sum([Total Amount]) desc )

select top 1 seller_state, sum([Total Amount]) from Finalised_Records_1
group by seller_state
order by  sum([Total Amount]) asc

--region based revenue generation
select region,round(sum([Total Amount]),2) as total_amount from Finalised_Records_1
group by region

-- chat

--channel type transaction	and thgeir percentage
with cte as (select f.Channel,count(*) most_preferred_channel 
from Finalised_Records_1 f 
join customer_360 o 
on f.Customer_id=o.Customer_id
group by f.Channel
) , cte2 as(
SELECT SUM(most_preferred_channel) AS total_transactions FROM cte
)
SELECT 
cte.Channel,
cte.most_preferred_channel,
ROUND(cte.most_preferred_channel * 100.0 / cte2.total_transactions, 2) AS percentage
FROM cte 
CROSS JOIN cte2;

--avg rating of stores
with cte as (select avg(Avg_rating)avrg,Delivered_StoreID  from Finalised_Records_1
group by Delivered_StoreID)
select round(avg(avrg),2) from cte

--top 10 customer id that are contributing sales by revenue product
select top 10 Customer_id,Channel, sum([Total Amount])from Finalised_Records_1
group by Customer_id,Channel
order by  sum([Total Amount]) desc

--same product ordered maxm time that order id 
select top 10 order_id,count(product_id) from Finalised_Records_1
group by order_id
order by  count(product_id) desc

--top 5 cust with inactive days
SELECT top 5
CUSTOMER_ID, MAX(INACTIVE_DAYS) FROM customer_360
group by customer_id
order by MAX(INACTIVE_DAYS) desc

-- recent buyers
select top 10 Customer_id,datediff(day,Bill_date_timestamp,getdate()) days from Finalised_Records_1
group by Customer_id,datediff(day,Bill_date_timestamp,getdate())
order by datediff(day,Bill_date_timestamp,getdate()) asc

--customer with top 5 most profitable catagory 
select top 5 f.Customer_id,f.category,sum(o.Total_Amount-o.Total_cost) profit from _Orders360_	o join Finalised_Records_1 f
on o.Orderid=f.order_id
group by f.Customer_id,f.category
order by profit desc

--total amount
  select sum([Total Amount]) from Finalised_Records_1
  select sum(Total_expenditure) from customer_360
  select sum(Total_Amount) from _Orders360_

  select count(Customer_id) from customer_360
  where Tenure= 1

  ---max days of 10 customer id who tennure means staying attached with company
  select top 10 Customer_id,Tenure from customer_360
  order by Tenure desc

  --- weekend sales and weekday sales
  select sum(Weekday_Transactions) as weekday from customer_360 
  where Weekday_Transactions = 1
   union
select sum(Weekend_Transactions) as Weekend from customer_360
   where Weekend_Transactions = 1

   --yoy revenue growth

  with cte as( select year(f.Bill_date_timestamp) year,sum(f.[Total Amount]) crrent from Finalised_Records_1 f
   group by year(f.Bill_date_timestamp) ),cte2 as (
   select c.year,c.crrent,lag(crrent,1,crrent)over(order by year) as next_rev
   from cte c)
   select year,crrent,next_rev, ((cp.crrent-cp.next_rev) / cp.next_rev)*100 as growth_rate from cte2 cp

   -- mom revenue growth and percentage
   with cte as (
   select datepart(year,Bill_date_timestamp) as year,datepart(month,Bill_date_timestamp) as month,sum([Total Amount]) as revenue   
   from Finalised_Records_1
   group by datepart(year,Bill_date_timestamp),	datepart(month,Bill_date_timestamp)), cte2 as(
   select year,month,revenue,
   lag(revenue,1,revenue) over(order by month) as prev_rev from cte	)
   select year,month,((prev_rev-revenue)/prev_rev)*100 from cte2



  select * from _Orders360_
 select * from _store360_
 select * from Finalised_Records_1
 select * from customer_360

 --28 column
 select count(*) as no_of_columns
 from information_schema.COLUMNS
 where TABLE_NAME = 'customer_360'  ;
--28 column
 select count(*) as no_of_columns
 from information_schema.COLUMNS
 where TABLE_NAME = '_store360_'  ;
  --  19 column
   select count(*) as no_of_columns
 from information_schema.COLUMNS
 where TABLE_NAME = 'Finalised_Records_1'  ;
 ---19 columns
   select count(*) as no_of_columns
 from information_schema.COLUMNS
 where TABLE_NAME = '_Orders360_'  ;

 --yoy growth
 with cte as (select year(Bill_date_timestamp) year,sum([Total Amount]) money from Finalised_Records_1
 group by year(Bill_date_timestamp))
, cte2 as(
select *,	lag(money,1,money) over(order by year) prev_year from cte	)
select year,prev_year,ROUND(((money - prev_year) * 100.0 / prev_year), 2) from cte2 

-- top 10 expensive product
WITH product_revenue AS (
    SELECT 
    Product_ID,
    SUM([Total Amount]) AS revenue
    FROM Finalised_Records_1
    GROUP BY Product_ID
),
total_revenue AS (
    SELECT 
    SUM(revenue) AS total_rev
    FROM product_revenue
),
final_cte AS (
SELECT 
    p.Product_ID,
    p.revenue,
    ROUND(CAST(p.revenue AS FLOAT) / t.total_rev, 4) AS contribution_percent
FROM product_revenue p
CROSS JOIN total_revenue t
)
SELECT 
    Product_ID,
    revenue,
    contribution_percent
FROM final_cte
ORDER BY revenue DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;


-- deviding gender based on category
SELECT 
    Category,
    SUM(CASE WHEN Gender = 'm' THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN Gender = 'f' THEN 1 ELSE 0 END) AS female_count
FROM Finalised_Records_1
GROUP BY Category
order by female_count desc;


 ---top 10stores that are creating high revenue
 select top 10 Delivered_StoreID,sum([Total Amount])  from Finalised_Records_1
 group by Delivered_StoreID
 order by  sum([Total Amount]) desc

 -- category wise profit on customer city
 select top 10 category,customer_city, sum([Total Amount]-(Quantity*[Cost Per Unit])) from Finalised_Records_1
 group by category,customer_city
 order by  sum([Total Amount]-(Quantity*[Cost Per Unit])) desc

 --category wise mom growth
	 with cte as(
	 select  year(Bill_date_timestamp) year,category , month(Bill_date_timestamp) month, sum([Total Amount])	 exp
	 from Finalised_Records_1
	 group by category , month(Bill_date_timestamp),year(Bill_date_timestamp) ), cte2 as( 
	 select year,month,category,exp,
	 lag(exp,1,exp)over(order by month) prev from cte)
	 select exp,month,year,prev,(exp-prev)*100/prev as percentage from cte2
	 
	 


 -- avg rating by month
 select year(Bill_date_timestamp), month(Bill_date_timestamp),avg(Avg_rating)  from Finalised_Records_1
 group by  month(Bill_date_timestamp),year(Bill_date_timestamp)
 order by avg(Avg_rating) desc

 --avg rating by category
 select category, avg(Avg_rating)  from Finalised_Records_1
 group by Category
 order by avg(Avg_rating) desc

 -- avg rating by state
 select customer_state, avg(Avg_rating) from Finalised_Records_1
 group by customer_state
 order by avg(Avg_rating) desc

 --count of category demand per state
 with cte as (
select customer_state,[Total Amount], row_number() over(partition by category order by customer_state) as x
from Finalised_Records_1
) select customer_state,count(x)  from cte 
group by  customer_state
order by  count(x) desc

-- category ales on day
SELECT 
   category	, DATENAME(WEEKDAY, Bill_date_timestamp),
    COUNT(DATENAME(WEEKDAY, Bill_date_timestamp)) AS Category_Count
FROM Finalised_Records_1
GROUP BY category,DATENAME(WEEKDAY, Bill_date_timestamp)	
ORDER BY COUNT(DATENAME(WEEKDAY, Bill_date_timestamp)) desc 
      -- Optional: to order Sunday–Saturday

--- customer who purchase in multiple category from different state  that percentage
 with cte as (
 select customer_state,  COUNT( category)  from Finalised_Records_1
 group by customer_state
 having count(category) >1)
 select category, count(state),count(Customer_id) from cte
 group by 	category

 -- sales by payment method 

 select o.Channel_used,count(f.Customer_id) from _Orders360_	o join Finalised_Records_1 f
 on o.Orderid=f.order_id
 group by o.Channel_used
 order by count(f.Customer_id) desc

--month over month growth percentage
WITH cte AS (
    SELECT 
        DATEPART(YEAR, Bill_date_timestamp) AS year,
        DATENAME(MONTH, Bill_date_timestamp) AS month_name,
        DATEPART(MONTH, Bill_date_timestamp) AS month_number,
        SUM([Total Amount]) AS amount
    FROM Finalised_Records_1
    GROUP BY 
        DATEPART(YEAR, Bill_date_timestamp),
        DATENAME(MONTH, Bill_date_timestamp),
        DATEPART(MONTH, Bill_date_timestamp)
),
cte2 AS (
    SELECT 
        year,
        month_name,
        month_number,
        amount,
        LAG(amount) OVER (PARTITION BY year ORDER BY month_number) AS prev_amount
    FROM cte
)
SELECT 
    year,
    month_name,
    amount,
    prev_amount,
    ROUND((amount - prev_amount) * 100.0 / NULLIF(prev_amount, 0), 2) AS month_over_month_change
FROM cte2
ORDER BY year, month_number;


--- top 5 profit making categories
select top 5 category,sum([Total Amount]-Quantity*[Cost Per Unit])as profit 
from Finalised_Records_1 
group by Category
order by  sum([Total Amount]-Quantity*[Cost Per Unit]) desc

-- top 5 revenue gerating category
select top 5 category,sum([Total Amount])as profit 
from Finalised_Records_1 
group by Category
order by  sum([Total Amount]) desc

-- mom growth rate in percentage
with cte as (
select datepart(year,Bill_date_timestamp) year,datename(month,Bill_date_timestamp) month, sum([Total Amount]) amount
from Finalised_Records_1
group by  datepart(year,Bill_date_timestamp) ,datename(month,Bill_date_timestamp) ),cte2 as(
select 	year,month,amount, 
lag(amount,1,amount) over(order by amount) as prev from cte	)
select month,round((amount - prev)/amount*100,2) from cte2

--yoy growth rate
with cte as (select datepart(YEAR,Bill_date_timestamp)year,
sum([Total Amount]) amount from Finalised_Records_1
group by datepart(YEAR,Bill_date_timestamp)),
cte2 as(
select YEAR, amount,
lead(amount,1,amount)over(order by year desc) prev from cte )
select year,round((amount - prev)/amount*100,2) from cte2


--week over week growth percentage
with cte as(
select datepart(week,Bill_date_timestamp) week, datepart(month,Bill_date_timestamp) month,
datepart(year,Bill_date_timestamp) year,
sum([Total Amount]) amount
from Finalised_Records_1
group by   datepart(week,Bill_date_timestamp) , datepart(month,Bill_date_timestamp) ,
datepart(year,Bill_date_timestamp) 
), cte2 as(select week,month,year, amount,
lag(amount,1,amount)over(order by week) prev_week from cte	)
select month,round(((amount- prev_week)* 100)/prev_week ,2) from cte2

select * from Customers
select * from _Orders360_
select * from ['Stores_Info']
select * from Finalised_Records_1

 -- rfm segmentation  
WITH RFM AS (
    SELECT 
        Customer_id,
        MAX(Bill_date_timestamp) AS LastPurchaseDate,
        COUNT(*) AS Frequency,
        SUM([Total Amount]) AS Monetary
    FROM Finalised_Records_1
    GROUP BY Customer_id
),
RecencyCalc AS (
    SELECT 
        Customer_id,
        DATEDIFF(DAY, LastPurchaseDate, (SELECT MAX(Bill_date_timestamp) FROM Finalised_Records_1)) AS Recency,
        Frequency,
        Monetary
    FROM RFM
),
RFM_Scores AS (
    SELECT 
        Customer_id,
        NTILE(4) OVER (ORDER BY Recency ASC) AS R_Score,
        NTILE(4) OVER (ORDER BY Frequency DESC) AS F_Score,
        NTILE(4) OVER (ORDER BY Monetary DESC) AS M_Score
    FROM RecencyCalc
),
FinalRFM AS (
    SELECT 
        Customer_id,
        CONCAT(R_Score, F_Score, M_Score) AS RFM_Score,
        CASE 
            WHEN R_Score >= 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Premium'
            WHEN R_Score >= 2 AND F_Score >= 2 AND M_Score >= 2 THEN 'Gold'
            WHEN R_Score = 2 AND (F_Score >= 1 OR M_Score >= 1) THEN 'Silver'
            ELSE 'Standard'
        END AS Segment
    FROM RFM_Scores
) 

--  Final step: Calculate revenue by Segment
SELECT 
    F.Segment,
    SUM(T.[Total Amount]) AS Total_Revenue,
    COUNT(DISTINCT T.Customer_id) AS No_of_Customers
FROM
    Finalised_Records_1 T
INNER JOIN 
    FinalRFM F
ON 
    T.Customer_id = F.Customer_id
GROUP BY 
    F.Segment
ORDER BY 
    Total_Revenue DESC;

	----Segment the customers (divide the customers into groups) based on the revenue
With Rev_percent AS(
SELECT *,
    PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY [Total Amount]) OVER () AS percentile_33,
    PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY [Total Amount]) OVER () AS percentile_66,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY [Total Amount]) OVER () AS percentile_99
FROM
   Finalised_Records_1
),
Cust_segment AS(
Select Customer_id,sum([Total Amount]) as revenue,
case  when sum([Total Amount])<Min(percentile_33) then 'low_revenue'
      WHEN SUM([Total Amount]) BETWEEN MIN(percentile_33) AND MIN(percentile_66) THEN 'medium_revenue'
      WHEN SUM([Total Amount]) BETWEEN MIN(percentile_66) AND MIN(percentile_99) THEN 'high_revenue'
      ELSE 'very_high_revenue'
end as customer_segment
from Rev_percent
group by Customer_id
),
Customer_count AS(
Select customer_segment,count(*) as customer_count from Cust_segment
group by customer_segment
)
Select *,Concat(ROund(customer_count*1.0/sum(customer_count) over()*100,2),'%') as percentage from Customer_count
order by customer_count desc

-- top 10 most expensive products belonging catergory
select top 10 category,  [Cost Per Unit]/sum([Cost Per Unit])*100 percentage from	Finalised_Records_1
order by product_id,[Cost Per Unit] desc

-- customer who did 1 transaction in 1 month

WITH FirstOrders AS (
    SELECT
    Customer_id,
    MIN(Bill_date_timestamp) AS FirstOrderDate ,
    FROM Finalised_Records_1
    GROUP BY Customer_id
	HAVING COUNT(*)>1
)
SELECT
    CAST(YEAR(FirstOrderDate) AS VARCHAR(4)) + '-' + 
    RIGHT('0' + CAST(MONTH(FirstOrderDate) AS VARCHAR(2)), 2) AS [Month],
    COUNT(*) AS CustomersAcquired
FROM FirstOrders
GROUP BY YEAR(FirstOrderDate), MONTH(FirstOrderDate)
ORDER BY [Month];


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






































