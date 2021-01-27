
drop table if exists DataScience.dbo.cb_results1lb11; 

with extracted_data as (
    select procdure,
        proc_short_desc,
        proc_cnt,
        conditon,
        conditon_desc,
        mbrs_w_proc_and_cond,
        mbrs_w_proc,
        confidence, 
        row_number() over (PARTITION BY key2 order by proc_cnt1 ) ID,
        proc_cnt1,
        key2,
        confidence1
from DataScience.dbo.cb_results1lb8
), compute_larger_detected as (
    select *,
           case when ID = 1 then 1
                when confidence1 > lag(confidence1) over (order by key2) then 1
                else 0
                end as larger_detected
    from extracted_data
)
select *,
       max(case when larger_detected = 1 then confidence1 end) over (partition by key2 order by ID) as monotonically_increasing
       into DataScience.dbo.cb_results1lb11
from compute_larger_detected

drop table if exists DataScience.dbo.cb_results1lb12; 
select 
ID
,proc_cnt1
,key2
,monotonically_increasing as confidence
into DataScience.dbo.cb_results1lb12
from DataScience.dbo.cb_results1lb11;

select * from DataScience.dbo.cb_results1lb12;--2625


------

;with proc_cnt1 as (
select * from ( values ('1'),('2'),('3'),('4'),('5'),('6+')) series(level)
)
, cte2 AS (
 select key2 , min(proc_cnt1) as mincnt
 from DataScience.dbo.cb_results1lb12
 group by key2
) 
, cte3 as (
select *
from proc_cnt1
cross apply cte2
where level >= mincnt
)
select level proc_cnt1
, c.key2 
, COALESCE ( confidence , max(confidence) over (partition by c.key2 order by level)) confidence
into DataScience.dbo.cb_results1lb13
from cte3 c
left join DataScience.dbo.cb_results1lb12 t
on c.key2 = t.key2
and  c.level = t.proc_cnt1
order by c.key2 , level
GO
