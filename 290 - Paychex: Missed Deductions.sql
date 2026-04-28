with data as (
  
  select 
  -- effective date on deduction
    d.pay_date, p.check_date, 
    ep.net as check_net_amount,  
    ifnull(json_value(ep.additional_data, '$.deductions[0].isBlocked'),0) is_blocked,
    m.company_code as company_id, m.location as employer, 
    m.first_name, m.last_name, cs.scored_at, cs.paystub_salary,
      
    ifnull(ed.status,'active') as connection_status, m.employee_id worker_id, 
    m.employment_status, m.pay_frequency, tda.avg_days_between_transactions,
    
    tdd.transaction_dates,
    
    json_value(m.additional_data, '$.payStandards[0].payFrequency') as paychex_frequency,
    TIMESTAMPDIFF(MONTH, m.start_date, now()) as tenure_months, salary, 
    x.paystub_count, x.last_paystub_date, x.all_pay_dates,
    
    di.created_at as deduction_created_at,
    p.completed_at as pay_period_close_at,
    di.amount as deduction_amount, ri.amount as remitted_amount, 
    
    case
        when ri.amount >= di.amount then 'paid'
        when ri.amount > 0 then 'short'
        else 'missed'
    end as paid,
    p.pay_period_id, p.status, p.num_checks as payperiod_check_count, 
    json_value(p.raw_data, '$.description') as payperiod_description,
    cast(json_value(p.raw_data, '$.startDate') as date) as payperiod_start_date,
    cast(json_value(p.raw_data, '$.endDate') as date) as payperiod_end_date,
    cast(json_value(p.raw_data, '$.submitByDate') as date) as payperiod_submit_by_date,
    
    lcp.last_employer_paystub_date,
    json_value(p.raw_data, '$.intervalCode') as payperiod_interval_code,
    (select count(1) from employee_paystubs where employee_manifest_id = m.id and pay_date = p.check_date) as matching_paystub,
    
    so.purchase_date
    
  from deduction d 
  inner join deduction_item di on di.deduction_id = d.entity_id 
  inner join employee_manifest m on m.customer_id = di.customer_id 
  inner join remittance r on r.deduction_id = d.entity_id
  left join remittance_item ri on ri.customer_id = di.customer_id and ri.remittance_id = r.entity_id 
  left join employers.company_payperiods p on p.company_id = m.company_code
    and p.check_date > date_sub(d.pay_date, interval 2 day) and p.check_date < date_add(d.pay_date, interval 2 day)
    and substring(json_value(p.raw_data, '$.intervalCode'), 1, 1) = substring(m.pay_frequency, 1, 1)
  left join employer_department ed on ed.department_prefix = m.company_code and ed.employer_id = m.employer_id 
  left join (
    select ep.employee_manifest_id, count(1)  as paystub_count, max(pay_date) as last_paystub_date, group_concat(pay_date SEPARATOR ', ') as all_pay_dates
    from employee_paystubs ep 
    inner join employee_manifest m1 on m1.id = ep.employee_manifest_id 
    where m1.employer_id=227 and ep.net > 0 and ep.pay_date is not null
    group by ep.employee_manifest_id 
  ) x on x.employee_manifest_id = m.id
  left join (
    select cp.company_id, max(cp.check_date) last_employer_paystub_date
    from employers.company_payperiods cp 
    where cp.num_checks > 0
    group by cp.company_id
  ) lcp on lcp.company_id = m.company_code 
  left join (
    select customer_id, min(created_at) as scored_at, max(json_value(data, '$.paycheckData.paystubSalary')) as paystub_salary
    from account.customer_score 
    where credit_limit > 0
    group by customer_id 
  ) cs on cs.customer_id = m.customer_id
  left join (
   SELECT 
    a.customer_id,
        CASE 
            WHEN COUNT(DISTINCT l.transaction_date) < 2 THEN NULL
            ELSE DATEDIFF(MAX(l.transaction_date), MIN(l.transaction_date)) 
                 / (COUNT(DISTINCT l.transaction_date) - 1)
        END AS avg_days_between_transactions,
        COUNT(DISTINCT l.transaction_date) AS transaction_count
    FROM agreements a
    INNER JOIN ledger l ON l.agreement_id = a.id
    WHERE l.status IN ('scheduled', 'past_due', 'payroll_pending')
    GROUP BY a.customer_id
    ORDER BY a.customer_id
  ) tda on tda.customer_id = m.customer_id 
  left join (
    select a.customer_id, group_concat(distinct l.transaction_date SEPARATOR ', ') as transaction_dates
    from agreements a
    inner join ledger l on l.agreement_id = a.id
    where l.status in ('scheduled', 'past_due', 'payroll_pending') and l.transaction_date < '2026-08-01' and a.employer_id=227
    group by a.customer_id
  ) tdd on tdd.customer_id = m.customer_id 
  
  left join employee_paystubs ep on ep.employee_manifest_id = m.id and ep.pay_date = p.check_date
  
  left join ( 
    select min(created_at) purchase_date, customer_id
    from sales_order 
    where employer_id = 227
    group by customer_id 
  ) so on so.customer_id = m.customer_id
   
  where d.employer_id = 227 -- and ifnull(ri.amount, 0) = 0
  order by d.entity_id

)

select *,
  case 
    when deduction_amount = remitted_amount then 'Payment Success - No Issues'
    when data.remitted_amount > 0 then 'Payment Partial Success - Remittance not Equal'
    when data.connection_status = 'disconnected' then 'company disconnected'
    when data.employment_status = 'Terminated' then 'employee terminated'
    when data.is_blocked = 1 then 'blocked'    
    when data.matching_paystub > 0 then 'deduction too late?'
    when data.matching_paystub = 0 and last_paystub_date is null then 'no paystubs found for employee'
    when data.matching_paystub = 0 and last_paystub_date < date(sysdate() - INTERVAL 33 day) then 'employee not paid - no paystub for over 1 month'
    when data.matching_paystub = 0 then 'employee not paid this cycle'
    else 'unknown'
  end as reason
  
from data
