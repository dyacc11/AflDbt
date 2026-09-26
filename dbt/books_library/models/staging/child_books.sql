{{ config(materialized='table') }}

SELECT
    book_id,
    title,
    author,
    genre,
    published_year,
    pages,
    language
FROM {{ ref('books') }}
WHERE target_audience = 'children'
