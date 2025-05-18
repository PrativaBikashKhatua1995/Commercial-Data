CREATE DATABASE SWIGGY_ORDERS;
USE SWIGGY_ORDERS;

CREATE TABLE USERS(
user_id	 INT PRIMARY KEY,
name CHAR (50),
email VARCHAR (100),
password NCHAR (100));

INSERT INTO  USERS
VALUES
('1','Nitish','nitish@gmail.com','p252h'),
('2','Khushboo','khushboo@gmail.com','hxn9b'),
('3','Vartika','vartika@gmail.com','9hu7j'),
('4','Ankit','ankit@gmail.com','lkko3'),
('5','Neha','neha@gmail.com','3i7qm'),
('6','Anupama','anupama@gmail.com','46rdw2'),
('7','Rishabh','rishabh@gmail.com','4sw123');

CREATE TABLE RESTURANTS
(r_id INT,
r_name CHAR (100),
cuisine VARCHAR(50));

INSERT INTO RESTURANTS(r_id,r_name,cuisine)
VALUES 
(1,'dominos','Italian'),
(2,'kfc','American'),
(3,'box8','North Indian'),
(4,'Dosa Plaza','South Indian'),
(5,'China Town','Chinese');

CREATE TABLE FOOD
(f_id INT,
f_name VARCHAR (50),
type VARCHAR (20));

INSERT INTO FOOD
VALUES 
('1','Non-veg Pizza','Non-veg'),
('2','Veg Pizza','Veg'),
('3','Choco Lava cake','Veg'),
('4','Chicken Wings','Non-veg'),
('5','Chicken Popcorn','Non-veg'),
('6','Rice Meal','Veg'),
('7','Roti meal','Veg'),
('8','Masala Dosa','Veg'),
('9','Rava Idli','Veg'),
('10','Schezwan Noodles','Veg'),
('11','Veg Manchurian','Veg');

CREATE TABLE MENU (
menu_id INT PRIMARY KEY,
r_id INT,
f_id INT,
price INT
) ;

INSERT INTO MENU 
VALUES 
(1,	1,	1,	450),
(2,	1,	2,	400),
(3,	1,	3,	100),
(4,	2,	3,	115),
(5,	2,	4,	230),
(6,	2,	5,	300),
(7,	3,	3,	80),
(8,	3,	6,	160),
(9,	3,	7,	140),
(10,	4,	6,	230),
(11,	4,	8,	180),
(12,	4,	9,	120),
(13,	5,	6,	250),
(14,	5,	10,	220),
(15,	5,	11,	180)
;

CREATE TABLE ORDERS (
order_id	INT PRIMARY KEY,
user_id INT,
r_id INT,
amount INT,
date DATE,
partner_id INT,
delivery_time INT,
delivery_rating  INT,
restaurant_rating INT NULL) ;

INSERT INTO ORDERS (order_id,user_id ,r_id ,amount ,date ,partner_id,delivery_time ,delivery_rating ,restaurant_rating ) 
VALUES 
(1003,	1,	3,	240,	'2022-06-15',	5,	29,	4,  NULL),
(1004,	1,	3,	240,	'2022-06-29',	4,	42,	3,	5),
(1005,	1,	3,	220,	'2022-07-10',	1,	58,	1,	4),
(1006,	2,	1,	950,	'2022-06-10',	2,	16,	5,  NULL),	
(1007,	2,	2,	530,	'2022-06-23',	3,	60,	1,	5),
(1008,	2,	3,	240,	'2022-07-07',	5,	33,	4,	5),
(1009,	2,	4,	300,	'2022-07-17',	4,	41,	1,  NULL),	
(1010,	2,	5,	650,	'2022-07-31',	1,	67,	1,	4),
(1011,	3,	1,	450,	'2022-05-10',	2,	25,	3,	1),
(1012,	3,	4,	180,	'2022-05-20',	5,	33,	4,	1),
(1013,	3,	2,	230,	'2022-05-30',	4,	45,	3,	NULL),
(1014,	3,	2,	230,	'2022-06-11',	2,	55,	1,	2),
(1015,	3,	2,	230,	'2022-06-22',	3,	21,	5,	NULL),
(1016,	4,	4,	300,	'2022-05-15',	3,	31,	5,	5),
(1017,	4,	4,	300,	'2022-05-30',	1,	50,	1,	NULL),
(1018,	4,	4,	400,	'2022-06-15',	2,	40,	3,	5),
(1019,	4,	5,	400,	'2022-06-30',	1,	70,	2,	4),
(1020,	4,	5,	400,	'2022-07-15',	3,	26,	5,	3),
(1021,	5,	1,	550,	'2022-07-01',	5,	22,	2,	NULL),
(1022,	5,	1,	550,	'2022-07-08',	1,	34,	5,	1),
(1023,	5,	2,	645,	'2022-07-15',	4,	38,	5,	1),
(1024,	5,	2,	645,	'2022-07-21',	2,	58,	2,	1),
(1025,	5,	2,	645,	'2022-07-28',	2,	44,	4,	NULL)
;

CREATE TABLE DELIVERY_PARTNER (partner_id INT,
partner_name CHAR (20));

INSERT INTO DELIVERY_PARTNER (partner_id,	partner_name)
VALUES
(1,	'Suresh'),
(2,	'Amit'),
(3,	'Lokesh'),
(4,	'Kartik'),
(5,	'Gyandeep');

CREATE TABLE ORDER_DETAILS(id INT PRIMARY KEY,
order_id INT,
f_id INT);

INSERT INTO ORDER_DETAILS
VALUES 
(1,	1001,	1),
(2,	1001,	3),
(3,	1002,	4),
(4,	1002,	3),
(5,	1003,	6),
(6,	1003,	3),
(7,	1004,	6),
(8,	1004,	3),
(9,	1005,	7),
(10,	1005,	3),
(11,	1006,	1),
(12,	1006,	2),
(13,	1006,	3),
(14,	1007,	4),
(15,	1007,	3),
(16,	1008,	6),
(17,	1008,	3),
(18,	1009,	8),
(19,	1009,	9),
(20,	1010,	10),
(21,	1010,	11),
(22,	1010,	6),
(23,	1011,	1),
(24,	1012,	8),
(25,	1013,	4),
(26,	1014,	4),
(27,	1015,	4),
(28,	1016,	8),
(29,	1016,	9),
(30,	1017,	8),
(31,	1017,	9),
(32,	1018,	10),
(33,	1018,	11),
(34,	1019,	10),
(35,	1019,	11),
(36,	1020,	10),
(37,	1020,	11),
(38,	1021,	1),
(39,	1021,	3),
(40,	1022,	1),
(41,	1022,	3),
(42,	1023,	3),
(43,	1023,	4),
(44,	1023,	5),
(45,	1024,	3),
(46,	1024,	4),
(47,	1024,	5),
(48,	1025,	3),
(49,	1025,	4),
(50,	1025,	5);

SELECT* FROM USERS;
SELECT * FROM ORDER_DETAILS;
SELECT * FROM ORDERS;
SELECT * FROM DELIVERY_PARTNER;
SELECT * FROM FOOD;
SELECT * FROM MENU;
SELECT * FROM RESTURANTS;

--find customers who have never ordered
--average price/dish
--find top resturants in terms of no of orders for a given month
--resturant with monthly sales > x for
--show all orders with order details for a perticular customer in a perticular date range
--find resturants with repeted customers
-- month over month revenue growth of swiggy
--customer -> favorite food

--1.find customers who have never ordered
select name from users 
 where USER_ID not in
(select USER_ID from ORDERS);

--2.average price per dish

select f.f_name, avg(m.price) as avg_price
from menu m join food f 
on m.f_id=f.f_id
group by f.f_name;

--3.find top resturants in terms of no of orders for a given month
SELECT  TOP 1(RESTURANTS.r_name),COUNT(*) AS MONTHNUMBER
FROM ORDERS, RESTURANTS
WHERE ORDERS.r_id=RESTURANTS.r_id AND MONTH(date) LIKE 6 
GROUP BY RESTURANTS .r_name
ORDER BY COUNT(*) DESC
;


 --4.resturant with monthly sales > x for
SELECT R.r_name,SUM(amount) AS 'revenue'
FROM ORDERS O,RESTURANTS R
WHERE O.r_id=R.r_id AND MONTH(date) LIKE 6
GROUP BY R.r_name
HAVING SUM(amount) > 500;

select r.r_name, sum(amount) as revenue
from RESTURANTS r join orders o
on r.r_id=o.r_id and month(date) like 6
group by r.r_name
having sum(amount) > 500;


--5.show all orders with order details for a perticular customer in a perticular date range
SELECT O.order_id,R.r_name,F.f_name
FROM ORDERS O,RESTURANTS R,ORDER_DETAILS OD,FOOD F
WHERE user_id= ( SELECT user_id FROM USERS
WHERE name LIKE 'ANKIT') 
AND O.r_id=R.r_id 
AND O.order_id=OD.order_id
AND F.f_id=OD.f_id
AND date > '2022-06-10' AND date < '2022-07-10'
;

--find resturants with repeted customers
SELECT TOP 1 R.r_name, COUNT(T.user_id) AS loyal_customers
FROM(
SELECT r_id,user_id
FROM ORDERS
GROUP BY r_id ,user_id
HAVING COUNT(*) > 1) t
JOIN RESTURANTS R
ON R.r_id=T.r_id
GROUP BY R.r_id 
ORDER BY loyal_customers DESC
;

-- month over month revenue growth of swiggy
SELECT MONTH,((revenue-prev)/prev)*100 
FROM (
WITH sales AS 
(
	SELECT MONTH(date) AS 'MONTH',SUM(amount) AS 'revenue'
	FROM ORDERS 
	GROUP BY MONTH(date)
	ORDER BY MONTH ASC;
)
SELECT MONTH,revenue,LAG(revenue,1) OVER (ORDER BY revenue) AS PREV 
FROM sales) T;

----customer favorite --> food
WITH temp AS (
SELECT O.user_id,OD.F_ID,COUNT(*) AS frequency
  FROM ORDERS O
  JOIN ORDER_DETAILS OD
  ON O.ORDER_ID=OD.ORDER_ID
  GROUP BY O.user_id,OD.f_id
  )t
SELECT * FROM  (
SELECT O.user_id,OD.F_ID,COUNT(*) AS frequency
  FROM ORDERS O
  JOIN ORDER_DETAILS OD
  ON O.ORDER_ID=OD.ORDER_ID
  GROUP BY O.user_id,OD.f_id
  )  T1
 SELECT* FROM temp T1.frequency= (SELECT MAX(COUNT(*))
  FROM temp T2 
  WHERE T2.user_id=T1.user_id));
 