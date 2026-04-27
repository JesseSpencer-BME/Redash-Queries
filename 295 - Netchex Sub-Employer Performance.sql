WITH
params AS (
  SELECT
    COALESCE(STR_TO_DATE('{{ from_date }}','%Y-%m-%d'), '1900-01-01') AS from_dt
),

TestAccounts as (
    select cei.entity_id as customer_id
    from customer_entity_int cei
    inner join eav_attribute a on a.attribute_id = cei.attribute_id 
        and a.attribute_code = 'is_test_account'
        and cei.value = 1
),

SalesData AS (
    SELECT 
        so.employer_id,
        em.company_code,
        em.location,        
        COUNT(*) AS orders,
        COUNT(DISTINCT so.customer_id) AS unique_purchasers,
        SUM(so.total_due + so.total_paid) AS gross_sales
    FROM sales_order so
    INNER JOIN employee_manifest em on so.customer_id = em.customer_id
    CROSS JOIN params p
    WHERE 
        so.status != 'canceled'
        AND so.created_at >= p.from_dt
        and so.customer_id not in (select customer_id from TestAccounts)
        and so.employer_id = 205
    GROUP BY so.employer_id,
    em.company_code,
    em.location
),

RegisteredData AS (
    SELECT 
        ce.employer_id,
        m.company_code,
        m.location,
        COUNT(*) AS registered_employees
    FROM customer_entity ce
    inner join employee_manifest m on m.customer_id = ce.entity_id
    CROSS JOIN params p
    WHERE 
        ce.created_at >= p.from_dt
        and m.employment_status='active'
        and ce.entity_id not in (
            select customer_id from TestAccounts
        )
    GROUP BY ce.employer_id,
    m.company_code,
    m.location
), 

TerminatedBalance AS (
    SELECT 
        s.employer_id,
        m.company_code,
        m.location,        
        ROUND(SUM(l.amount),2) AS terminated_balance 
    FROM ledger l 
    INNER JOIN ledger_statuses ls 
        ON ls.status = l.status AND ls.for_balance = 1
    INNER JOIN agreements a 
        ON a.id = l.agreement_id
    INNER JOIN sales_order o 
        ON o.entity_id = a.order_id
    INNER JOIN customer_grid_flat cg 
        ON cg.entity_id = o.customer_id AND cg.employment_status = 2
    INNER JOIN employer s 
        ON s.employer_id = o.employer_id
    INNER JOIN employee_manifest m
        ON a.customer_id = m.customer_id
    CROSS JOIN params p
    WHERE 
        o.created_at >= p.from_dt
    GROUP BY s.employer_id,
    m.company_code,
    m.location
), 

EmployerData AS (
    SELECT 
        s.employer_id,
        s.name,
        coalesce(em.company_code,'unknown') as company_code,
        coalesce(em.location,'unknown') as location,
        coalesce(json_value(em.additional_data, '$.organization.name'),'unknown') as organization_name,
        COUNT(em.id) AS eligible_employees,
        COALESCE(r.registered_employees, 0) AS registered,
        COALESCE(sd.orders, 0) AS orders,
        COALESCE(sd.unique_purchasers, 0) AS unique_purchasers,
        COALESCE(sd.gross_sales, 0) AS gross_sales,
        COALESCE(tb.terminated_balance, 0) AS terminated_balance
    FROM employee_manifest em
    INNER JOIN employer s ON s.employer_id = em.employer_id
    LEFT JOIN criteria c ON c.id = IFNULL(s.criteria_id, 11)
    LEFT JOIN RegisteredData r 
      ON r.employer_id = s.employer_id and r.company_code = em.company_code and r.location = em.location
    LEFT JOIN SalesData sd 
      ON sd.employer_id = s.employer_id and sd.company_code = em.company_code and sd.location = em.location
    LEFT JOIN TerminatedBalance tb 
      ON tb.employer_id = s.employer_id and tb.company_code = em.company_code and tb.location = em.location
    WHERE 
        TIMESTAMPDIFF(YEAR, em.dob, CURRENT_DATE()) >= 18
        AND em.employment_status = 'Active'
        AND (em.employment_schedule != 'PRN' OR s.prn_eligible = 1)
        AND (em.employment_schedule != 'CT'  OR s.ct_eligible  = 1)
        AND (em.employment_schedule != 'PT'  OR s.pt_eligible  = 1)
        AND s.employer_id = 205
        
    GROUP BY s.employer_id, s.name, em.company_code, em.location
)

SELECT
    employer_id AS `Employer Id`,
    company_code,
    location,
    organization_name,
    name AS `Name`,
    eligible_employees AS `Eligible Employees`,
    registered AS `Registered`,
    registered / NULLIF(eligible_employees,0) * 100 AS `Registration Rate`,
    orders AS `Orders`,
    unique_purchasers AS `Unique Purchasers`,
    unique_purchasers / NULLIF(eligible_employees,0) * 100 AS `Total Usage Rate`,
    unique_purchasers / NULLIF(registered,0) * 100 AS `Registered Usage Rate`,
    gross_sales AS `Gross Sales`,
    gross_sales / NULLIF(eligible_employees,0) AS `$ per EE`,
    gross_sales / NULLIF(unique_purchasers,0) AS `$ per Purchaser`,
    gross_sales / NULLIF(orders,0) AS `AOV`,
    terminated_balance AS `Terminated Balance`,
    terminated_balance / NULLIF(gross_sales,0) * 100 AS `Terminated Bal. %`
FROM EmployerData
-- WHERE employer_id NOT IN ({{ Exclude Employer IDs }})  -- < not needed for single employer

UNION ALL

SELECT 
    NULL AS `Employer Id`,
    'Total'   AS `Name`,
    'Total' as company_code,
    'Total' as location,
    'Total' as organization_name,
    SUM(eligible_employees) AS `Eligible Employees`,
    SUM(registered)         AS `Registered`,
    SUM(registered) / NULLIF(SUM(eligible_employees),0) * 100 AS `Registration Rate`,
    SUM(orders)             AS `Orders`,
    SUM(unique_purchasers)  AS `Unique Purchasers`,
    SUM(unique_purchasers) / NULLIF(SUM(eligible_employees),0) * 100 AS `Total Usage Rate`,
    SUM(unique_purchasers) / NULLIF(SUM(registered),0) * 100 AS `Registered Usage Rate`,
    SUM(gross_sales)        AS `Gross Sales`,
    SUM(gross_sales) / NULLIF(SUM(eligible_employees),0) AS `$ per EE`,
    SUM(gross_sales) / NULLIF(SUM(unique_purchasers),0)  AS `$ per Purchaser`,
    SUM(gross_sales) / NULLIF(SUM(orders),0)             AS `AOV`,
    SUM(terminated_balance) AS `Terminated Balance`,
    SUM(terminated_balance) / NULLIF(SUM(gross_sales),0) * 100 AS `Terminated Bal. %`
FROM EmployerData;
