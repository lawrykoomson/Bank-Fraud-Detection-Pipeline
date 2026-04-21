/*
  Mart Model: mart_fraud_by_channel
  ===================================
  Aggregates fraud metrics by banking channel.
  Powers channel fraud analysis in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_fraud_transactions') }}

),

channel_fraud as (

    select
        channel,

        count(transaction_id)                               as total_transactions,
        count(case when requires_review then 1 end)         as flagged_transactions,
        count(case when is_fraud_actual = 1 then 1 end)     as actual_fraud_count,

        round(
            count(case when requires_review then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                                as flag_rate_pct,

        round(
            count(case when is_fraud_actual = 1 then 1 end)::numeric
            / nullif(count(transaction_id), 0) * 100
        , 2)                                                as actual_fraud_rate_pct,

        round(sum(transaction_amount_ghs), 2)               as total_volume_ghs,
        round(sum(case when requires_review
                       then transaction_amount_ghs end), 2) as flagged_value_ghs,
        round(avg(fraud_risk_score), 2)                     as avg_risk_score,

        count(case when is_high_amount then 1 end)          as high_amount_flags,
        count(case when is_foreign_ip then 1 end)           as foreign_ip_flags,
        count(case when is_unusual_hour then 1 end)         as unusual_hour_flags,
        count(case when is_rapid_txn then 1 end)            as rapid_txn_flags,

        round(
            sum(case when requires_review then transaction_amount_ghs end)
            / sum(sum(case when requires_review
                           then transaction_amount_ghs end)) over () * 100
        , 2)                                                as flagged_value_share_pct

    from staged
    group by channel

)

select * from channel_fraud
order by flagged_value_ghs desc