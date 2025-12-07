import duckdb

folder = "Day05"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    header = False,
    columns = {
        "database": "VARCHAR"
    }
)

sql_input_a = """--sql
    select 
        string_split(database,'-')[1]::bigint as range_start,
        string_split(database,'-')[2]::bigint as range_end
    from 
        raw_input
    where 
        contains(database, '-')
"""
ranges = duckdb.sql(sql_input_a)
# ranges.show()

sql_input_b = """--sql
    select 
        database::bigint as id
    from 
        raw_input
    where
        not contains(database, '-')
"""
ingredient_ids = duckdb.sql(sql_input_b)
# ingredient_ids.show()


sql_part1 = """--sql
    with fresh_ingredients as (
        select 
            ids.id
        from 
            ingredient_ids as ids
            semi join ranges
                on (
                    ids.id between ranges.range_start and ranges.range_end
                )
    )

    select count(id) as answer_part1 from fresh_ingredients
"""
duckdb.sql(sql_part1).show()


sql_part2 = """--sql
    with edge_points as (
        select range_start as edge from ranges
        union all 
        select range_end as edge from ranges
    ),
    unique_edge_points as (
        select edge from edge_points group by edge
    ),
    new_ranges as (
        select 
            edge + 1 as range_start,
            lead(edge, 1) over (order by edge) - 1 as range_end
        from 
            unique_edge_points
        union all 
        select
            edge as range_start,
            edge as range_end
        from unique_edge_points
    ),
    fresh_ranges as (
        select 
            new_ranges.range_start,
            new_ranges.range_end,
            new_ranges.range_end - new_ranges.range_start + 1 as id_count
        from 
            new_ranges
            semi join ranges
                on (
                    new_ranges.range_start between ranges.range_start and ranges.range_end
                )
        where
            new_ranges.range_end is not null
    )

    select sum(id_count) as answer_part2 from fresh_ranges
"""
duckdb.sql(sql_part2).show()
