{% snapshot payment_method_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='payment_method_id',
    strategy='check',
    check_cols=[
        'display_name',
        'provider',
        'type'
    ],
    file_format='delta'
) }}

SELECT
    payment_method_id,
    display_name,
    provider,
    type
FROM {{ ref('payment_method') }}

{% endsnapshot %}
