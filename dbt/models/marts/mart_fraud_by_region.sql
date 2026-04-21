/*
  Mart Model: mart_fraud_by_region
  ==================================
  Aggregates fraud metrics by Ghana region.
  Powers regional fraud heatmap in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_fraud_transactions') }}

),

region_fraud as (

    select
        region,

        count(transaction_id)                               as total_transactions,
        count(case when requires_review then 1 end)         as flagged_transactions,
        count(case when is_fraud_actual = 1 then 1 end)     as actual_fraud_count,

        round(
            count(case when requires_review then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                                as flag_rate_pct,

        round(sum(transaction_amount_ghs), 2)               as total_volume_ghs,
        round(sum(case when requires_review
                       then transaction_amount_ghs end), 2) as flagged_value_ghs,
        round(avg(fraud_risk_score), 2)                     as avg_risk_score,
        round(avg(transaction_amount_ghs), 2)               as avg_transaction_ghs,

        count(case when is_high_amount then 1 end)          as high_amount_count,
        count(case when is_foreign_ip then 1 end)           as foreign_ip_count,
        count(case when is_unusual_hour then 1 end)         as unusual_hour_count,

        round(
            sum(case when requires_review then transaction_amount_ghs end)
            / sum(sum(case when requires_review
                           then transaction_amount_ghs end)) over () * 100
        , 2)                                                as flagged_share_pct

    from staged
    group by region

)

select * from region_fraud
order by flagged_value_ghs desc