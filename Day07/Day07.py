import duckdb

folder = "Day07"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    header = False,
    columns = {
        "diagram": "VARCHAR"
    }
)
# raw_input.show()

sql_input = """--sql
    with map_rows as (
        select 
            row_number() over () as row_index,
            string_split(diagram, '') as row_list,
            apply(row_list, lambda x, i: [i::varchar,x]) as row_list_with_col
        from
            raw_input
    ),
    map as (
        select 
            row_index,
            unnest(row_list_with_col)[1]::integer as col_index,
            unnest(row_list_with_col)[2] as grid_value,
            case 
                when grid_value = 'S'
                    then true
                else false
            end as is_start_beam,
            case
                when grid_value = '^'
                    then true
                else false
            end as is_splitter
        from 
            map_rows
    )

    select * from map
"""
input = duckdb.sql(sql_input)
# input.show()


sql_part1a = """--sql
    with splitters as (
        select 
            row_index,
            col_index,
            unnest([col_index - 1, col_index + 1]) as to_col_index
        from 
            input
        where 
            is_splitter
    )

    select * from splitters
"""
part1a = duckdb.sql(sql_part1a)
# part1a.show()

sql_part1b = """--sql
    with recursive follow_beam(
        row_depth,
        beam_position
    ) as (
        -- base case 
        select 
            1 as row_depth,
            col_index as beam_position
        from
            input
        where 
            row_index = 1
            and is_start_beam
    union all 
        -- recursive step
        select 
            beams.row_depth + 1 as row_depth,
            case 
                when splitters.col_index is null -- no splitter
                    then beams.beam_position -- move straight
                else 
                    splitters.to_col_index -- split
            end as beam_position
        from 
            (select * from follow_beam group by all) as beams
            left join part1a as splitters
                on (
                    beams.row_depth + 1 = splitters.row_index
                    and beams.beam_position = splitters.col_index
                )
        where 
            row_depth < (select count(*) from raw_input)
    )

    select 
        row_depth,
        beam_position
    from 
        follow_beam 
    group by 
        row_depth,
        beam_position
    order by 
        row_depth, beam_position
"""
part1b = duckdb.sql(sql_part1b)
# part1b.show()

sql_part1c = """--sql
    select
        count(distinct [splitters.row_index, splitters.col_index]) as answer_part1
    from 
        part1a as splitters
        inner join part1b as beams
            on (
                beams.row_depth + 1 = splitters.row_index
                and beams.beam_position = splitters.col_index
            )
"""
duckdb.sql(sql_part1c).show()


sql_part2 = """--sql
    with recursive make_beam_path(
        row_depth,
        beam_position,
        timelines
    ) as (
        -- base case 
        select 
            1 as row_depth,
            col_index as beam_position,
            1::bigint as timelines
        from
            input
        where 
            row_index = 1
            and is_start_beam
    union all 
        -- recursive step
        select 
            beams.row_depth + 1 as row_depth,
            case 
                when splitters.col_index is null -- no splitter
                    then beams.beam_position -- move straight
                else 
                    splitters.to_col_index -- split
            end as beam_position,
            beams.timelines * beams.new_timelines as timelines
        from 
            (select row_depth, beam_position, timelines, count(*) as new_timelines from make_beam_path group by all) as beams
            left join part1a as splitters
                on (
                    beams.row_depth + 1 = splitters.row_index
                    and beams.beam_position = splitters.col_index
                )
        where 
            row_depth < (select count(*) from raw_input)
    )

    select sum(timelines) as answer_part2
    from make_beam_path
    where row_depth = (select count(*) from raw_input)
"""
duckdb.sql(sql_part2).show()
