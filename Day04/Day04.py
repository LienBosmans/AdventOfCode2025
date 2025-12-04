import duckdb
# Note: you'll need a newer version of DuckDB to run the lambda functions with Pyhon-style syntax (lambda x : x + 1).
# See https://duckdb.org/docs/stable/sql/functions/lambda for deprecation notice of single arrow syntax (x -> x + 1).
# print(duckdb.__version__)

folder = "Day04"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    header = False,
    columns = {
        "map_row": "VARCHAR"
    }
)

sql_input = """--sql
    with map_rows as (
        select 
            row_number() over () as row_index,
            string_split(map_row, '') as map_row_list
        from
            raw_input
    ),
    map_cols as (
        select 
            row_index,
            apply(map_row_list, lambda x, i: [x,i::varchar]) as map_row_list_with_col_info
        from
            map_rows
    ),
    map as (
        select 
            row_index,
            unnest(map_row_list_with_col_info)[2]::integer as col_index,
            unnest(map_row_list_with_col_info)[1] as grid_value,
            case when grid_value = '@' then 1 else 0 end as is_roll
        from 
            map_cols
    )

    select * from map
"""
input = duckdb.sql(sql_input)
# input.show()

sql_visualization_input = """--sql
    select 
        row_index,
        array_to_string(list(grid_value order by col_index), '') as map_row
    from 
        input
    group by 
        row_index
    order by
        row_index
"""
# duckdb.sql(sql_visualization_input).show()


sql_part1 = """--sql
    with rolls as (
        select 
            roll.row_index,
            roll.col_index,
            'x' as grid_value,
            count(adjacent_rolls) as count_adjacent_rolls
        from 
            input as roll
            left join input as adjacent_rolls
                on (
                    adjacent_rolls.is_roll = 1
                    and adjacent_rolls.col_index between (roll.col_index - 1) and (roll.col_index + 1)
                    and adjacent_rolls.row_index between (roll.row_index - 1) and (roll.row_index + 1)
                    and [adjacent_rolls.col_index, adjacent_rolls.row_index] != [roll.col_index, roll.row_index]
                )
        where 
            roll.is_roll = 1
        group by 
            roll.row_index, 
            roll.col_index
    ),
    visualization as (
        select 
            map.row_index,
            array_to_string(list(coalesce(rolls.grid_value, map.grid_value) order by map.col_index), '') as map_row
        from 
            input as map
            left join rolls
                on (
                    rolls.count_adjacent_rolls < 4 
                    and map.row_index = rolls.row_index
                    and map.col_index = rolls.col_index
                ) 
        group by 
            map.row_index
    )

    -- select * from visualization order by row_index
    select count(grid_value) as answer_part1 from rolls where count_adjacent_rolls < 4
"""

duckdb.sql(sql_part1).show()


sql_part2a = """--sql
    with roll_only_grid as (
        select 
            roll.row_index as roll_row,
            roll.col_index as roll_col,
            a_roll.row_index as a_roll_row,
            a_roll.col_index as a_roll_col
        from 
            input as roll
            left join input as a_roll
                on (
                    a_roll.is_roll = 1
                    and a_roll.col_index between (roll.col_index - 1) and (roll.col_index + 1)
                    and a_roll.row_index between (roll.row_index - 1) and (roll.row_index + 1)
                    and [a_roll.col_index, a_roll.row_index] != [roll.col_index, roll.row_index]
                )
        where 
            roll.is_roll = 1
    )

    select * from roll_only_grid
"""
roll_only_grid = duckdb.sql(sql_part2a)


sql_part2b = """--sql
    with recursive roll_list(
        recursion_depth,
        row_index,
        col_index,
        prev_is_removed,
        is_removed,
        prev_removals,
        total_removals
    ) as (
        -- base case
            select 
                0 as recursion_depth,
                grid.roll_row as row_index,
                grid.roll_col as col_index,
                false as prev_is_removed,
                count(*) < 4 as is_removed,
                -1 as prev_removals,
                0 as total_removals
            from 
                roll_only_grid as grid
            group by
                grid.roll_row,
                grid.roll_col
        union all 
        -- recursive step
            select 
                roll.recursion_depth + 1 as recursion_depth,
                roll.row_index as row_index,
                roll.col_index as col_index,
                roll.is_removed as prev_is_removed,
                count(*) filter (r_a_roll.is_removed is null) < 4 as is_removed,
                roll.total_removals as prev_removals,
                roll.total_removals + count(roll.is_removed) filter (roll.is_removed and not roll.prev_is_removed) over () as total_removals
            from 
                roll_list as roll
                left join roll_only_grid as grid
                    on (
                        roll.is_removed = 0
                        and roll.row_index = grid.roll_row
                        and roll.col_index = grid.roll_col
                    )
                left join roll_list as r_a_roll
                    on ( -- to remove adjacent rolls from grid that were already removed
                        r_a_roll.is_removed
                        and grid.a_roll_row = r_a_roll.row_index
                        and grid.a_roll_col = r_a_roll.col_index
                    )
            group by 
                roll.recursion_depth,
                roll.row_index,
                roll.col_index,
                roll.prev_is_removed,
                roll.is_removed,
                roll.total_removals,
                roll.prev_removals
            having 
                roll.recursion_depth < 1000 -- avoid infinite loops stop criterium
                and roll.prev_removals < roll.total_removals -- only continue if extra rolls were removed
    )

    -- select * from roll_list where is_removed order by recursion_depth, row_index, col_index
    select max(total_removals) as answer_part2 from roll_list
"""

duckdb.sql(sql_part2b).show()
