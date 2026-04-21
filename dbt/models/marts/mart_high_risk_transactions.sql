/*
  Mart Model: mart_high_risk_transactions
  =========================================
  Surfaces all High and Critical risk transactions
  for compliance team review in Power BI.

  Grain: One row per flagged transaction
  Author: Lawrence Koomson
*/

with staged as (

    select * from {{ ref('stg_fraud_transactions') }}

),

high_risk as (

    select
        transaction_id,
        transaction_date,
        transaction_hour,
        day_of_week,
        is_weekend,

        sender_account,
        receiver_account,
        transaction_amount_ghs,
        transaction_type,
        channel,
        region,
        currency,
        transaction_status,
        ip_country,

        fraud_risk_score,
        fraud_risk_tier,
        risk_label,
        signals_triggered,
        requires_review,
        alert_reason,

        is_high_amount,
        is_rapid_txn,
        is_amount_outlier,
        is_unusual_hour,
        is_foreign_ip,
        is_high_frequency,
        amount_z_score,
        mins_since_last_txn,

        is_fraud_actual,
        processed_at,

        rank() over (
            order by fraud_risk_score desc,
                     transaction_amount_ghs desc
        )                                               as risk_rank

    from staged
    where requires_review = true

)

select * from high_risk
order by risk_rank