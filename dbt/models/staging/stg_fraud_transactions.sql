/*
  Staging Model: stg_fraud_transactions
  =======================================
  Cleans and standardises raw bank fraud transaction data.
  Single source of truth for all downstream fraud models.

  Source: fraud_detection.fact_transactions
  Author: Lawrence Koomson
*/

with source as (

    select * from {{ source('fraud_detection', 'fact_transactions') }}

),

staged as (

    select
        transaction_id,
        timestamp                                       as transaction_timestamp,
        txn_date                                        as transaction_date,
        txn_hour                                        as transaction_hour,
        txn_day_of_week                                 as day_of_week,
        is_weekend,

        upper(trim(sender_account))                     as sender_account,
        upper(trim(receiver_account))                   as receiver_account,

        amount_ghs                                      as transaction_amount_ghs,
        upper(transaction_type)                         as transaction_type,
        upper(channel)                                  as channel,
        initcap(region)                                 as region,
        upper(currency)                                 as currency,
        upper(status)                                   as transaction_status,
        ip_country,

        is_unusual_hour,
        is_high_amount,
        amount_z_score,
        is_amount_outlier,
        coalesce(mins_since_last_txn, 0)                as mins_since_last_txn,
        is_rapid_txn,
        daily_txn_count,
        is_high_frequency,
        is_foreign_ip,
        is_self_transfer,

        fraud_risk_score,
        upper(fraud_risk_tier)                          as fraud_risk_tier,
        requires_review,
        alert_reason,
        is_fraud_actual,
        processed_at,

        case
            when fraud_risk_score >= 71 then 'Critical'
            when fraud_risk_score >= 46 then 'High'
            when fraud_risk_score >= 21 then 'Medium'
            else 'Low'
        end                                             as risk_label,

        (is_high_amount::int +
         is_rapid_txn::int +
         is_amount_outlier::int +
         is_unusual_hour::int +
         is_foreign_ip::int +
         is_high_frequency::int)                        as signals_triggered

    from source
    where transaction_id is not null
      and amount_ghs > 0

)

select * from staged
