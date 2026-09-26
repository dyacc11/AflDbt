{{ config(materialized='view') }}

SELECT
    book_id,
    title,
    author,
    genre,
    published_year,
    pages,
    language,
    CASE
        WHEN published_year >= 2020 THEN 'Very Recent'
        WHEN published_year >= 2000 THEN 'Recent'
        ELSE 'Classic'
    END AS era
FROM {{ ref('child_books') }}
WHERE published_year >= 2000
