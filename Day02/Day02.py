import duckdb

folder = "Day02"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    sep = "####", # use dummy sep so everything is ingested in one column
    header = False,
    columns = {
        "input_string": "VARCHAR"
    }
)

sql_input = """--sql
    with range_strings as (
        select 
            unnest(string_split(input_string, ',')) as range_string
        from
            raw_input
    ),
    ranges as (
        select 
            range_string,
            string_split(range_string, '-')[1]::bigint as range_from,
            string_split(range_string, '-')[2]::bigint as range_until
        from 
            range_strings
    )

    select * from ranges
"""

input = duckdb.sql(sql_input)
# input.show()

sql_part1 = """--sql
    with max_digits as (
        select 
            len(max(range_until)::varchar) as max_digits,
            ceil(max_digits/2)::bigint as max_repeat,
            1 as start_series,
            repeat('9', max_repeat)::bigint as stop_series
        from 
            input
    ),
    ids_to_repeat as (
        select
            generate_series as id_to_repeat
        from 
            generate_series(
                (select start_series from max_digits),
                (select stop_series from max_digits)
            )
    ),
    repeated_ids as (
        select 
            concat(id_to_repeat::varchar,id_to_repeat::varchar)::bigint as id
        from 
            ids_to_repeat
    ),
    invalid_ids as (
        select 
            input.range_string,
            input.range_from,
            input.range_until,
            repeated_ids.id as invalid_id
        from 
            input
            inner join repeated_ids
                on (
                    input.range_from <= repeated_ids.id
                    and repeated_ids.id <= input.range_until
                )
    )

    select sum(invalid_id) as answer_part1 from invalid_ids
"""

duckdb.sql(sql_part1).show()


sql_part2 = """--sql
    with max_digits as (
        select 
            len(max(range_until)::varchar) as max_digits,
            ceil(max_digits/2)::bigint as max_repeat,
            1 as start_series,
            repeat('9', max_repeat)::bigint as stop_series            
        from 
            input
    ),
    ids_to_repeat as (
        select
            generate_series as id_to_repeat,
            (select max_digits from max_digits)//len(id_to_repeat::varchar) as max_repeat
        from 
            generate_series(
                (select start_series from max_digits),
                (select stop_series from max_digits)
            )
    ),
    unnest_repeats as (
        select 
            id_to_repeat,
            unnest(generate_series(2, max_repeat)) as repeat_count
        from 
            ids_to_repeat
    ),
    repeated_ids as (
        select
            repeat(id_to_repeat::varchar, repeat_count)::bigint as id
        from 
            unnest_repeats
        group by id -- remove duplicates (f.e. 2222 = repeat(22,2) = repeat(2,4))
    ),
    invalid_ids as (
        select 
            input.range_string,
            input.range_from,
            input.range_until,
            repeated_ids.id as invalid_id
        from 
            input
            inner join repeated_ids
                on (
                    input.range_from <= repeated_ids.id
                    and repeated_ids.id <= input.range_until
                )
    )

    select sum(invalid_id) as answer_part2 from invalid_ids
"""

duckdb.sql(sql_part2).show()
