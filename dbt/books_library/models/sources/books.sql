{{ config(materialized='view') }}

SELECT 1  AS book_id, 'Charlotte''s Web'       AS title, 'E.B. White'         AS author, 'Fantasy'   AS genre, 'children' AS target_audience, 1952 AS published_year, 184  AS pages, 'English' AS language UNION ALL
SELECT 2,     'The Hobbit',                       'J.R.R. Tolkien',     'Fantasy',   'children', 1937, 310, 'English' UNION ALL
SELECT 3,     'Harry Potter and the Sorcerer''s Stone', 'J.K. Rowling',       'Fantasy',   'children', 1997, 309, 'English' UNION ALL
SELECT 4,     'The Little Prince',                'Antoine de Saint-Exupery', 'Fiction', 'children', 1943, 96,  'French'  UNION ALL
SELECT 5,     'Matilda',                          'Roald Dahl',           'Fiction',   'children', 1988, 240, 'English' UNION ALL
SELECT 6,     'Where the Wild Things Are',        'Maurice Sendak',       'Picture Book','children', 1963, 48, 'English' UNION ALL
SELECT 7,     'The Very Hungry Caterpillar',      'Eric Carle',           'Picture Book','children', 2020, 26, 'English' UNION ALL
SELECT 8,     'Dog Man',                          'Dav Pilkey',           'Graphic Novel','children', 2021, 240, 'English' UNION ALL
SELECT 9,     'Dune',                             'Frank Herbert',        'Sci-Fi',    'adult', 1965, 412, 'English' UNION ALL
SELECT 10,    '1984',                             'George Orwell',        'Dystopian', 'adult', 1949, 328, 'English' UNION ALL
SELECT 11,    'Project Hail Mary',                'Andy Weir',            'Sci-Fi',    'adult', 2021, 476, 'English' UNION ALL
SELECT 12,    'Klara and the Sun',                'Kazuo Ishiguro',       'Fiction',   'adult', 2021, 288, 'English' UNION ALL
SELECT 13,    'Piranesi',                         'Susanna Clarke',       'Fantasy',   'adult', 2020, 245, 'English' UNION ALL
SELECT 14,    'The Midnight Library',             'Matt Haig',            'Fiction',   'adult', 2020, 288, 'English' UNION ALL
SELECT 15,    'Le Petit Prince',                  'Antoine de Saint-Exupery', 'Fiction','adult', 1943, 96, 'French'
