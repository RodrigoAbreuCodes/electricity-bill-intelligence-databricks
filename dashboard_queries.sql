-- bills
SELECT b.bill_id, b.template, b.marketer_name AS company, b.billing_end_date,
       date_format(b.billing_end_date, 'yyyy-MM') AS year_month,
       b.total_eur, b.energy_kwh, b.eur_per_kwh,
       CAST(b.needs_review AS INT) AS in_review
FROM fact_bill b;
-- field_accuracy
SELECT bill_id, template, field, is_correct, confidence
FROM fact_field_accuracy
WHERE is_scored;
-- ai_errors
SELECT bill_id, template, CAST(needs_review AS INT) AS caught
FROM fact_bill
WHERE has_ai_error;
-- review_queue
SELECT bill_id, template, review_reasons, min_confidence, total_eur
FROM gold_review_queue;
