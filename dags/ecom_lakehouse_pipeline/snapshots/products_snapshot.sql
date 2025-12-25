{% snapshot products_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='check',
    check_cols=[
        'product_name',
        'product_description',
        'unit_price',
        'category_name',
        'brand_name'
    ],
    file_format='delta'
) }}

SELECT
    product_id,
    product_name,
    product_description,
    unit_price,
    category_id,
    category_name,
    category_description,
    brand_id,
    brand_name,
    brand_origin,
    created_at,
    updated_at
FROM {{ ref('products') }}

{% endsnapshot %}
