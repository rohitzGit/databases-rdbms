# https://mode.com/sql-tutorial/sql-sub-queries/
'''
select
    REF_DATE,
    TRANSACTION_ID,
    RISK_PARTNER_ID,
    RWA,
    PARTNER_NAME,
    /*sum(RWA) over (PARTITION BY REF_DATE,
                                RISK_PARTNER_ID,
                                TRANSACTION_ID) as sum_RWA,*/
    RWA -
    LAG(RWA, 1) OVER (PARTITION BY RISK_PARTNER_ID,
                                    TRANSACTION_ID ORDER BY REF_DATE) as diff_rwa
/*  ROW_NUMBER() over(PARTITION BY REF_DATE,
                                   RISK_PARTNER_ID,
                                   TRANSACTION_ID
                      ORDER BY REF_DATE) as row_num*/
FROM amundi_austria_d
--where TRANSACTION_ID='5639098'
 
/*
SELECT
    RISK_PARTNER_ID,
    TRANSACTION_ID,
    sum(case when REF_DATE='202203' then RWA else NULL end) as March22,
    sum(case when REF_DATE='202204' then RWA else NULL end) as April22,
    sum(case when REF_DATE='202205' then RWA else NULL end) as May22
from
    amundi_austria_d
group by RISK_PARTNER_ID, TRANSACTION_ID*/
 
CREATE TABLE clean_crm_data_nostro AS
SELECT
     DATA,
     CO_CUS,
     CO_DEAL,
     CO_BUSINESS_LINE,
     CO_BUSINESS_LINE_TK,
     DL_BUSINESS_LINE_TK,
     CO_PRODUCT_CDG_LEV3,
     DL_PRODUCT_CDG_LEV3,
     CO_PRODUCT_CDG,
     DL_PRODUCT_CDG,
    sum(CASE WHEN CO_VALUE_TYPE_LEV2='1_RW   ' THEN AMOUNT_OTP ELSE NULL END) AS RWA,
    sum(CASE WHEN CO_VALUE_TYPE_LEV2='1_V12  ' THEN AMOUNT_OTP ELSE NULL END) AS EXPOSURE
FROM
    (
        SELECT  
            *
        FROM
            crm_data_nostro
        WHERE
            CO_VALUE_TYPE_LEV2 in ('1_RW   ', '1_V12  ')
    )
GROUP BY 1,2,3,4,5,6,7,8,9,10
 

'''



# https://bipp.io/sql-tutorial/advanced-sql/sql-correlated-subqueries/
 
-- List the employees who have never received a bonus.
select *
from employee
where employee_id != (
                        SELECT
                            e.employee_id
                        from
                            employee e
                        join payment_history ph
                        on e.employee_id=ph.employee_id
                        where payment_type='bonus'
                     )
 
-- List the employees who have never received a bonus.
-- correlated subquery
/*The main difference between correlated and simple subqueries is that correlated subqueries reference columns from the outer table. In the example, ph.employee_id = e1.employee_id is a reference to the outer subquery table*/
/*The negative part of the data question is often solved in the correlated subquery by using a NOT EXISTS operator in the WHERE clause. EXISTS is an operator always followed by a subquery. If the subquery returns at least one record, then the EXISTS evaluates to TRUE. If the subquery returns empty, then the EXISTS evaluates to FALSE. Note we use NOT EXISTS, which is the opposite to EXISTS.*/
select
*
from employee e
where not EXISTS (
                        SELECT
                            ph.employee_id
                        from
                            payment_history ph
                        where e.employee_id=ph.employee_id
                        and ph.payment_type='bonus'
                     )
 
-- join for positive queries, eg: List the employees who have received a bonus.
-- exists/non exists for negative queries
    -- exists ~ employee_id=
    -- not EXISTS ~ employee_id!=
 

-- the employee names who made a higher salary in March 2018 than their average monthly salary for all previous months.
 
SELECT first_name, last_name
FROM employee e1, payment_history ph
WHERE e1.employee_id = ph.employee_id
AND amount_paid > = (
                                       SELECT avg(amount_paid)
                                        FROM  payment_histoty ph2
                                        WHERE ph2.employee_id = e1.employee_id
                                              AND ph2.payment_date < ‘2018-03-01’
                                        )
AND month(ph.payment_date) = 3
AND year(ph.payment_date) = 2018
AND ph.payment_type = ‘salary’
 
/* The term window refers to the set of rows used to calculate the result of the function.
SQL Window functions enable you to calculate functions including: average, count, max, and min on a group of records. The individual records are not collapsed, so you can create a query combining or showing the individual record together with the result of a window function. */
 
-- find the salary of each employee and the highest salary in his/her department.
-- for each department, what is the highest salary
SELECT
ew.*,
max(ew.salary) OVER(PARTITION by department) as max_dept_sal
from
employee_window ew
 
--Suppose you want a report with the value of every share with the previous value and the variation percentage.
SELECT
s.*,
lag(value, 1) over (PARTITION by share_symbol order by timestamp) as prev_val,
round(((value - (lag(value) OVER (PARTITION BY share_symbol ORDER BY timestamp )))*100)/value,3) as prev_val_perc
from share_def s
 
-- rank salary for each department:
select *,
rank() over (PARTITION by department order by salary desc) as rank_dept_sal
from employee_window
 
-- metric for every employee about how close they are from the top salary of their department.
 
SELECT
ew.employee_id,
ew.full_name,
ew.department,
ew.salary,
ew.salary/max(salary) over (partition by department order by salary desc) as max_dept_sal
from employee_window ew
 
-- time_to_next_station
SELECT
    train_id,
    station,
    time as "station_time",
    lead(time) OVER (PARTITION BY train_id ORDER BY time) - time
        AS time_to_next_station
FROM train
ORDER BY 1 , 3;
 
-- important for joins on for , only b = (not a & not (a&b))
 
-- all women irrespective of whether they are married or not
SELECT
        W.first_name AS woman_first_name,
        W.last_name  AS woman_last_name,
        M.first_name AS man_first_name,
        M.last_name  AS man_last_name
FROM Woman W LEFT JOIN Man M ON W.marriage_id = M.marriage_id
 
-- women who are not married
SELECT
        W.first_name AS woman_first_name,
        W.last_name  AS woman_last_name,
        M.first_name AS man_first_name,
        M.last_name  AS man_last_name
FROM Woman W LEFT JOIN Man M ON W.marriage_id = M.marriage_id
WHERE  M.marriage_id IS NULL
 
-- women and men who are not married
SELECT
        W.first_name AS woman_first_name,
        W.last_name  AS woman_last_name,
        M.first_name AS man_first_name,
        M.last_name  AS man_last_name
FROM Woman W FULL JOIN Man M ON W.marriage_id = M.marriage_id
WHERE  M.marriage_id IS NULL or W.marriage_id IS NULL
 
/*. You want a query to obtain all the possible candidate pairs of products for the bundle meeting your criteria:
 
Products in stock with more than 100 units
Combined price of both products in the range $200 to $350*/
 
-- since the product_id appears to be int/varchar, it is ordered and can be used to comapre a product other than itself in a join operation
        -- compare each product with every other product to find a combination
-- query is correct but data type uplaoded is wrong for price hence prices dont add up
SELECT
       P1.name AS name_product1,
       P2.name AS name_product2,
       P1.price + P2.price AS bundle_price,
       P1.stock AS stock_product1,
       P2.stock AS stock_product2
FROM product P1
    INNER JOIN product P2 ON P1.product_id > P2.product_id
WHERE  P1.stock > 100
  AND  P2.stock > 100
  AND  P1.price + P2.price BETWEEN '200.0' AND '350.0'
 

--Cumulative sum
-- over() without PARTITION will create window over all columns one row at a time, eg- 1 row window, 2 row window,...
select
*,
sum(stock) over (order by product_id) as cum_sum_stock
from product
ORDER BY P1.name, P2.name ;
 

-- hvb cr
 
select *,
case when VALUE_TYPE='8620' then AMOUNT else 0.0 end AS RWA,
case when VALUE_TYPE='EX_ST' then AMOUNT else 0.0 end AS EXPOSURE
from hvb_cr
 
--to transpose/pivot
select RISK_PARTNER_ID,
sum(case when VALUE_TYPE='8620' then AMOUNT else 0.0 end) AS RWA,
sum(case when VALUE_TYPE='EX_ST' then AMOUNT else 0.0 end) AS EXPOSURE
from hvb_cr
group by RISK_PARTNER_ID
 
--
select
    RISIKOPARTNER_ID_GESCHAEFT,
    GESCHAEFT_ID,
    sum(case when POSITION_NUM_BASEL_2_0=9007 then RISIKOAKTIVA else 0.0 end) AS CPR,
    sum(case when POSITION_NUM_BASEL_2_0=9008 then RISIKOAKTIVA else 0.0 end) AS MR
from
    CPR_Dec22_seg
GROUP BY RISIKOPARTNER_ID_GESCHAEFT, GESCHAEFT_ID
 
-- first row per group
with hpg
as
(SELECT
*,
row_number() over (PARTITION by share_symbol order by value DESC) as high_per_group
from
share_def)
select * from hpg
where hpg.high_per_group=1
 

--find all employees that belong to the location 1700
-- joins are faster
select *
from employee
where department_id in (
    select department_id
    from
    department
    where location_id='1700'
    )
 
or
 
select
*
from employee e
inner join department d
on e.department_id = d.department_id
where d.location_id='1700'
 
--employees who have the highest salary:
 
with ranked_table as
(
    SELECT
        *,
        -- rank will skip a value if found two rows with same rank
        -- 1, 2, 2, 4
        rank() over (order by salary DESC) as ranked
    from
        employee
)
SELECT * from ranked_table where ranked=1 --2,3,4
 
or
 
SELECT
*
from
employee
where salary = (select max(salary)
                    FROM
                employee)
 
-- all employees who salaries are greater than the average salary of all employees
SELECT
    *
from employee
where salary > (
                    SELECT
                        avg(salary)
                    from
                        employee
                )
 
or
 
with cte as
(SELECT
    *,
    avg(salary) over() as avg_sal
FROM
    employee
    )
SELECT
    *
from cte
where salary>avg_sal
 
-- all departments which have at least one employee with the salary is greater than 10,000
-- one row from outer query goes into inner query and for that dept(since it is used in join condition), it is checked if there are employees who earn more than 10000 in that dept
-- if 'n' employee names satisfies this conidtion then it is subsituted by 1, since we are using EXISTS which does not require exact values to be provided, it is just a boolean operator
SELECT
    department_name
FROM
    department d
WHERE
    EXISTS( SELECT
            1
        FROM
            employee e
        WHERE
            salary > 10000
                AND e.department_id = d.department_id)
ORDER BY department_name;
 
-- The EXISTS operator checks for the existence of rows returned from the subquery. It returns true if the subquery contains any rows.
-- https://www.sqltutorial.org/sql-subquery/
-- The following condition evaluates to true if x is greater than every value returned by the subquery.
-- X>ALL (subquery)
SELECT
    employee_id, first_name, last_name, salary
FROM
    employees
WHERE
    salary >= ALL (SELECT
            MIN(salary)
        FROM
            employees
        GROUP BY department_id)
ORDER BY first_name , last_name
 
-- the following condition evaluates to true if x is greater than any value returned by the subquery. So the condition x > SOME (1,2,3) evaluates to true if x is greater than 1.
-- x > ALL (1,2,3) evaluates to true if x is greater than 1&2&3.
 
-- The outer query looks at these values and determines which employee’s salaries are greater than or equal to any highest salary by department.
SELECT
    employee_id, first_name, last_name, salary
FROM
    employees
WHERE
    salary >= SOME (SELECT
            MAX(salary)
        FROM
            employees
        GROUP BY department_id);
 
-- query as a subquery in the FROM clause to calculate the average of average salary of departments as follows
SELECT
    ROUND(AVG(average_salary), 0)
FROM
    (SELECT
        AVG(salary) average_salary
    FROM
        employees
    GROUP BY department_id) department_salary;
 
--finds the salaries of all employees, their average salary, and the difference between the salary of each employee and the average salary
SELECT
*,
avg(salary) over () as avg_sal,
round((salary - avg(salary) over ()),0) as sal_diff
from
employee
 
or
 
SELECT
    employee_id,
    first_name,
    last_name,
    (
        SELECT
            avg(salary)
        FROM
            employee
    ) as avg_salary,
    salary - (
                SELECT
                    avg(salary)
                FROM
                    employee
            ) sal_diff
from
    employee
 

# https://www.geeksforgeeks.org/difference-between-nested-subquery-correlated-subquery-and-join-operation/#:~:text=In%20Correlated%20query%2C%20a%20query,a%20common%20field%20between%20them.



--avg of all rows
avg(salary) over () as avg_sal -- as window function
-- or as single value
SELECT
    avg(salary)
FROM
    employee
 
-- all employees whose salary is higher than the average salary of the employees in their departments
-- For each employee, the database system has to execute the correlated subquery once to calculate the average salary of the employees in the department of the current employee.
 
with avg_sal_table as (
SELECT
    department_id,
    employee_id,
    first_name,
    last_name,
    salary,
    round(avg(salary) over (PARTITION by department_id),0) as avg_dept
from
    employee
order by department_id
)
SELECT * from avg_sal_table
where salary>avg_dept
order by
department_id,
first_name,
last_name
 
or
 
SELECT
    department_id,
    employee_id,
    first_name,
    last_name,
    salary
from
    employee e1
where
    salary > (
                select
                    avg(salary)
                from
                    employee e2
                where e1.department_id=e2.department_id
                )
order by
department_id,
first_name,
last_name
 
-- use order by in a window only for rank, dense rank, row num, lag, lead to avoid confusion
 

-- all employees who have no dependents
-- Loop
    -- for each record in employee(outer), take employee_id from one row, join with dependent, check if that employee exists in dependent, and return 1 if dependent present
    -- verify againsts boolean operator 'not exists', true or false, if any dependent was matched for an employee
 
SELECT
    department_id,
    employee_id,
    first_name,
    last_name
from
    employee e
where
    NOT EXISTS (
                select
                    1
                from
                    dependent d
                where e.employee_id=d.employee_id
                )
order by
first_name,
last_name
 
--
 
SELECT
*,
CASE    
    when salary>15000 then 'high'
    when salary<10000 and salary<=15000 then 'mid'
    else 'low'
END as indicator
from
employee
 

--
 
/*if over() is blank, then sum of column is appended
  if over(order by), then
        cum_sum is a good example because it will use
        one row WINDOW
        two row WINDOW
        .
        .
  if over(partition by .. order by ..), then
        over each partition, do,
        one row WINDOW
        two row WINDOW
        .
        .
        .
*/  
SELECT
*,
sum(salary) over (order by salary) as cum_sum
from
employee
 

/*When you have an ORDER BY clause, the function only operates on a window, that is, a subset of the result set, relative to the current row.
When you say "ORDER BY LEVEL", it will only operate on LEVELs less that or equal to the current LEVEL, so on
LEVEL = 1, the analytic fucntion will only look at LEVEL <= 1, that is, just 1; on
LEVEL = 2, the analytic fucntion will only look at LEVEL <= 2, that is, 1 and 2; on
LEVEL = 3, the analytic fucntion will only look at LEVEL <= 3, that is, 1, 2 and 3
...
LEVEL = 6, the analytic fucntion will only look at LEVEL <= 6, that is, 1, 2, 3, 4, 5 and 6
 
In the function call without the ORDER BY clause, the function looks at the entire result set, regrdless of what vlaue LEVEL has on the current row.*/
 

-- https://www.sqltutorial.org/seeit/  
-- sql editor with sample data
 

-- for each row(employee), if there are 3 employees with salary greater than the employee, then the employee's salary is the 3rd largest
-- =3 is the main clause which compares the count
SELECT first_name, salary
FROM employee A
WHERE 3 = (SELECT count(1)
             FROM employee B
             WHERE B.salary>A.salary)
 
-- filter 3 rows and select next row after 3rd row but does not work when duplicate salaries found
SELECT
*
from employee
order by salary desc limit 3,1
 

-- filter 3 rows and select next 5 rows after 3rd row but does not work when duplicate salaries found
SELECT
*
from employee
order by salary desc limit 3,1
 

-- join using subqueries
SELECT
    *
from
    employee e
left join (select
                department_id, sum(salary)
            from
                department
            group by
                department_id) d
on e.department_id=d.department_id
 

-- counting nulls
-- https://alexchang7a.medium.com/sql-advanced-cheatsheet-subqueries-f3741e08676e
SELECT COUNT(CASE WHEN is_shippable = 'YES' THEN 1 ELSE NULL END) *
       1.00 / COUNT(*) * 100 AS percent_shippable
FROM
(SELECT o.ID, CASE WHEN c.address IS NULL THEN 'NO' ELSE 'YES'  
         END AS is_shippable
 FROM orders o
 LEFT JOIN customers c
 ON o.customer_ID = c.ID) sub
 
-- count nulls and not nulls
select sum(case when a is null then 1 else 0 end) count_nulls
     , count(a) count_not_nulls
  from us;
 
-- count(1) is same as count(*)
-- count(1) will count nulls
-- count(col_name) will not count nulls
 
--cum_sum per dept
SELECT
    *,
    sum(salary) over (PARTITION by department_id order by salary) cum_sum
from
    employee e
 

-- pre-aggregation before joins through subqueries
-- explain
-- nested query/subquery and joins more efficient than correlated subquery
-- subquery
    -- nested query
    -- correlated query
-- avoid DISTINCT
-- avoid correlated SUBQUERIES
-- cost benefit of adding INDEX
-- consider materialized VIEWS
 