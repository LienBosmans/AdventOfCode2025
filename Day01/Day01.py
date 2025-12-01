import duckdb

folder = "Day01"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

input = duckdb.read_csv(
    input_file, 
    header = False,
    columns = {
        "rotation": "VARCHAR"
    }
)

sql_statement_part1 = """--sql
    with rotation_numbers as (
        select 
            row_number() over () as rowid,
            rotation,
            case 
                when left(rotation,1) = 'R'
                    then right(rotation, len(rotation) - 1)::integer
                when left(rotation,1) = 'L'
                    then -1 * right(rotation, len(rotation) - 1)::integer
            end as rotation_number
        from
            input    
    ),
    running_sum as (
        select 
            rowid,
            rotation,
            50 + sum(rotation_number) over (order by rowid) as running_sum,
            case when (running_sum % 100 = 0) then 1 else 0 end as flag
        from 
            rotation_numbers
    )

    select sum(flag) as answer_part1 from running_sum
"""

duckdb.sql(sql_statement_part1).show()


sql_statement_part2 = """--sql
    with rotation_numbers as (
        select 
            row_number() over () as rowid,
            rotation,
            case 
                when left(rotation,1) = 'R'
                    then right(rotation, len(rotation) - 1)::integer
                when left(rotation,1) = 'L'
                    then -1 * right(rotation, len(rotation) - 1)::integer
            end as rotation_number,
        from
            input    
    ),
    running_sum as (
        select 
            rowid,
            rotation,
            rotation_number,
            50 + sum(rotation_number) over (order by rowid) as running_sum,
            case 
                when running_sum % 100 < 0
                    then 100 + running_sum % 100
                else running_sum % 100
            end as new_position
        from 
            rotation_numbers
    ),
    prev_running_sum as (
        select 
            rowid,
            rotation,
            ifnull(lag(new_position, 1) over (order by rowid), 50) as prev_position,
            case
                when prev_position = 0 then 1
                else 0 
            end as prev_flag,
            rotation_number,
            new_position,
            case
                when new_position = 0 then 1
                else 0 
            end as flag,
            case
                when prev_flag = 1 or flag = 1
                    then abs(rotation_number) // 100
                else abs((prev_position + rotation_number) - (new_position)) // 100
            end as passes
        from
            running_sum
    )

    select sum(flag) + sum(passes) as answer_part2 from prev_running_sum
"""

duckdb.sql(sql_statement_part2).show()
