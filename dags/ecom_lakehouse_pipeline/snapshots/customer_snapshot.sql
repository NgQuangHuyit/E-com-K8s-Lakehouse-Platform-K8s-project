{% snapshot customers_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=[
        'first_name',
        'last_name',
        'email',
        'phone_number',
        'gender',
        'tire'
    ],
    file_format='delta'
) }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    gender,
    tire,
    created_at,
    updated_at
FROM {{ ref('customers') }}

{% endsnapshot %}
