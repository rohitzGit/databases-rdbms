-- Select

-- Find Second Highest Salary: 176

    -- Solution 1
    SELECT 
        MAX(salary) as SecondHighestSalary
    FROM 
        leetcode_easy.Employee
    WHERE salary != (SELECT MAX(salary) FROM Employee)

    -- Solution 2
    SELECT
        (SELECT DISTINCT
                Salary
            FROM
                Employee
            ORDER BY Salary DESC
            LIMIT 1 OFFSET 1) AS SecondHighestSalary
    ;

-- Nth highest salary: 177

    -- Solution 1
    CREATE FUNCTION getNthHighestSalary(N INT) RETURNS INT
    BEGIN
    RETURN (
        with ranked_table as
            (
                SELECT
                    salary,
                    dense_rank() over (order by salary desc) as ranked
                from 
                    employee
            )
            SELECT
                salary
            from 
                ranked_table 
            where ranked=n
            limit 1
    );
    END

    -- Solution 2
    CREATE FUNCTION getNthHighestSalary(N INT) RETURNS INT
    BEGIN
    DECLARE M INT; 
        SET M = N-1; 
    RETURN (
        SELECT DISTINCT salary
        FROM Employee
        ORDER BY salary DESC
        LIMIT M, 1
    );
    END

-- DENSE_RANK scores: 178

    -- Solution 1
    SELECT
        *,
        DENSE_RANK() OVER(ORDER BY score DESC) as 'rank'
    FROM
        Scores
    
    -- Solution 2 : Windows Dense Rank Implementation
    SELECT
        *,
        (
            SELECT
                COUNT(DISTINCT S2.score)
            FROM
                Scores S2
            WHERE S2.score >= S1.score
        ) as 'rank'
    FROM
        Scores S1
    ORDER BY S1.score DESC

    -- Solution 3 : Windows Dense Rank Implementation
    SELECT
        S1.id as S1_id,
        S1.score as S1_score,
        COUNT(DISTINCT S2.score) as 'rank'
    FROM
        Scores S1
    INNER JOIN Scores S2 ON S1.score<=S2.score
    GROUP BY S1_id, S1_score
    ORDER BY S1_id, S1_score

-- Consecutive numbers: 180

    -- Solution 1: Windows lag Implementation
    SELECT
        *
    FROM
        Logs l1,
        Logs l2,
        Logs l3
    WHERE l1.id = l2.id-1
    AND l2.id = l3.id-1
    AND l1.num = l2.num
    AND l2.num = l3.num

    -- Solution 2: Using lag
    SELECT
        *
    FROM
        (
            SELECT
                *,
                LAG(num, 1) OVER() as lag_1,
                LAG(num, 2) OVER() as lag_2
            FROM
                Logs
        ) lag_added
    WHERE 1=1
    AND lag_added.num=lag_added.lag_1
    AND lag_added.num=lag_added.lag_2

--

--