WITH _7D AS (

select 
    date
    ,namespace
    ,friendly_name
    ,category
    ,chain
    ,coingecko_id
    ,avg(transactions) as tx_7d
    ,avg(DAU) as DAU_7d
    ,avg(RETURNING_USERS) as RETURNING_USERS_7d
    ,avg(NEW_USERS) as NEW_USERS_7d
from ARTEMIS_ANALYTICS.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_NAMESPACE
where date <= DATEADD(DAY, -7, CURRENT_DATE())
group by 1,2,3,4,5,6

)

,_30D AS (

select 
    date
    ,namespace
    ,friendly_name
    ,category
    ,chain
    ,coingecko_id
    ,avg(transactions) as tx_30d
    ,avg(DAU) as DAU_30d
    ,avg(RETURNING_USERS) as RETURNING_USERS_30d
    ,avg(NEW_USERS) as NEW_USERS_30d
from ARTEMIS_ANALYTICS.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_NAMESPACE
where date <= DATEADD(DAY, -30, CURRENT_DATE())
group by 1,2,3,4,5,6

)



SELECT
    a.date
    ,a.namespace
    ,a.friendly_name
    ,a.category
    ,a.chain
    ,a.coingecko_id
    ,a.DAU_7d
    ,b.DAU_30d
    ,a.tx_7d
    ,b.tx_30d
    ,a.RETURNING_USERS_7d
    ,b.RETURNING_USERS_30d
    ,a.NEW_USERS_7d
    ,b.NEW_USERS_30d
FROM _7D a
LEFT JOIN _30D b ON A.DATE = B.DATE AND A.NAMESPACE = B.NAMESPACE