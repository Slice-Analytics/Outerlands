WITH _7D AS (

select 
    namespace
    ,avg(transactions) as avg_tx_7d
    ,avg(DAU) as avg_DAU_7d
    ,avg(RETURNING_USERS) as avg_RETURNING_USERS_7d
    ,avg(NEW_USERS) as avg_NEW_USERS_7d
from BAM_BY_NAMESPACE
where date >= DATEADD(DAY, -7, CURRENT_DATE())
group by 1

)

,_30D AS (

select 
    namespace
    ,avg(transactions) as avg_tx_30d
    ,avg(DAU) as avg_DAU_30d
    ,avg(RETURNING_USERS) as avg_RETURNING_USERS_30d
    ,avg(NEW_USERS) as avg_NEW_USERS_30d
from BAM_BY_NAMESPACE
where date >= DATEADD(DAY, -30, CURRENT_DATE())
--and namespace = 'gmx'
group by 1

)
,_7D_prev AS (

select 
    namespace
    ,avg(transactions) as avg_tx_7d_prev
    ,avg(DAU) as avg_DAU_7d_prev
    -- ,avg(RETURNING_USERS) as avg_RETURNING_USERS_7d
    -- ,avg(NEW_USERS) as avg_NEW_USERS_7d
from BAM_BY_NAMESPACE
where date >= DATEADD(DAY, -8, CURRENT_DATE()) and date < CURRENT_DATE()
group by 1

)

,_30D_prev AS (

select 
    namespace
    ,avg(transactions) as avg_tx_30d_prev
    ,avg(DAU) as avg_DAU_30d_prev
    -- ,avg(RETURNING_USERS) as avg_RETURNING_USERS_30d
    -- ,avg(NEW_USERS) as avg_NEW_USERS_30d
from BAM_BY_NAMESPACE
where date >= DATEADD(DAY, -31, CURRENT_DATE()) and date < CURRENT_DATE()
--and namespace = 'gmx'
group by 1

)



SELECT
    a.namespace
    ,a.avg_DAU_7d
    ,c.avg_DAU_7d_prev
    ,(a.avg_DAU_7d-c.avg_DAU_7d_prev)/c.avg_DAU_7d_prev as DAU_7dma
    ,b.avg_DAU_30d
    ,d.avg_DAU_30d_prev
    ,(b.avg_DAU_30d-d.avg_DAU_30d_prev)/d.avg_DAU_30d_prev as DAU_30dma
    ,a.avg_tx_7d
    ,c.avg_tx_7d_prev
    ,(a.avg_tx_7d-c.avg_tx_7d_prev)/c.avg_tx_7d_prev as tx_7dma
    ,b.avg_tx_30d
    ,d.avg_tx_30d_prev
    ,(b.avg_tx_30d-d.avg_tx_30d_prev)/d.avg_tx_30d_prev as tx_30dma
    ,a.avg_RETURNING_USERS_7d
    ,b.avg_RETURNING_USERS_30d
    ,a.avg_NEW_USERS_7d
    ,b.avg_NEW_USERS_30d
FROM _7D a
LEFT JOIN _30D b ON A.NAMESPACE = B.NAMESPACE
LEFT JOIN _7D_prev c ON A.NAMESPACE = c.NAMESPACE
LEFT JOIN _30D_prev d ON A.NAMESPACE = d.NAMESPACE