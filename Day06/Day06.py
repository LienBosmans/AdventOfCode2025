import duckdb

folder = "Day06"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    header = False,
    columns = {
        "homework": "VARCHAR"
    }
)

sql_input = """--sql
    with homework_rows as (
        select 
            row_number() over () as row_index,
            string_split_regex(trim(homework), '\s+') as homework_row
        from 
            raw_input
    ),
    homework_col_info as (
        select 
            row_index,
            apply(homework_row, lambda x, i: [x,i::varchar]) as homework_row_with_col_index
        from 
            homework_rows
    ),
    homework_grid as (
        select 
            row_index,
            unnest(homework_row_with_col_index)[2]::integer as col_index,
            unnest(homework_row_with_col_index)[1] as grid_value,
        from 
            homework_col_info
    ),
    homework_cols as (
        select
            col_index,
            list(grid_value order by row_index) as homework_col,
            list_filter(homework_col, lambda x: try_cast(x as integer))::integer[] as numbers,
            homework_col[-1] as operation            
        from 
            homework_grid
        group by 
            col_index
    )

    select numbers, operation from homework_cols
"""
input = duckdb.sql(sql_input)
# input.show()


sql_part1 = """--sql
    with homework_answers as (
        select
            numbers,
            operation,
            case 
                when operation = '+'
                    then list_sum(numbers)::bigint
                when operation = '*'
                    then list_product(numbers)::bigint
            end as answer
        from 
            input
    )

    select sum(answer) as answer_part1 from homework_answers
"""
duckdb.sql(sql_part1).show()


sql_input_part2 = """--sql
    with homework_rows as (
        select 
            row_number() over () as row_index,
            string_split(homework, '') as homework_row
        from 
            raw_input
    ),
    homework_col_info as (
        select 
        row_index,
        apply(homework_row, lambda x, i: [x,i::varchar]) as homework_row_with_col_index
        from 
            homework_rows
    ),
    homework_grid as (
        select 
            row_index,
            unnest(homework_row_with_col_index)[2]::integer as col_index,
            unnest(homework_row_with_col_index)[1] as grid_value,
        from 
            homework_col_info
    ),
    homework_cols as (
        select
            col_index,
            list(grid_value order by row_index) as homework_col,
            list_filter(homework_col, lambda x: try_cast(x as integer))::integer[] as numbers,
            array_to_string(numbers, '')::integer as number_value,
            case 
                when homework_col[-1] in ('+','*')
                    then homework_col[-1] 
                else null
            end as operation,
            case when operation is null then 0 else 1 end as is_new_problem,
            sum(is_new_problem) over (order by col_index) as problem_index  
        from 
            homework_grid
        group by 
            col_index
    ),
    problem_statements as (
        select 
            problem_index,
            first(operation) as operation,
            list(number_value) as numbers
        from 
            homework_cols
        where 
            number_value is not null
        group by 
            problem_index
    )

    select * from problem_statements order by problem_index desc
"""
input_part2 = duckdb.sql(sql_input_part2)
# input_part2.show()

sql_part2 = """--sql
    with homework_answers as (
        select
            numbers,
            operation,
            case 
                when operation = '+'
                    then list_sum(numbers)::bigint
                when operation = '*'
                    then list_product(numbers)::bigint
            end as answer
        from 
            input_part2
    )

    select sum(answer) as answer_part2 from homework_answers
"""
duckdb.sql(sql_part2).show()
