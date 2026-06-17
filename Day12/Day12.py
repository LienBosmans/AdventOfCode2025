import duckdb

folder = "Day12"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    sep = "🪿", # use dummy sep so everything is ingested in one column
    header = False,
    columns = {
        "puzzle_input": "VARCHAR"
    }
)

sql_input = """--sql
    with label_input_rows as (
        select 
            row_number() over () as row_number,
            puzzle_input,
            case
                when puzzle_input like '%x%:%' then 'region'
                when puzzle_input like '%:%' then 'shape_id'
                else 'shape'
            end as row_label,
            case
                when row_label = 'shape_id'
                    then string_split(puzzle_input,':')[1]::integer + 1
                else null
            end as shape_id,
            case 
                when row_label = 'region'
                    then string_split(string_split(puzzle_input, ':')[1],'x')::integer[]
                else null
            end as region_size,
            case
                when row_label = 'region'
                    then string_split(string_split(puzzle_input, ': ')[2], ' ')::integer[]
                else null
            end as region_shape_qty
        from
            raw_input
        where puzzle_input is not null
    ),
    forward_fill as (
        select 
            row_number,
            puzzle_input,
            row_label,
            case
                when row_label = 'region'
                    then null
                else sum(case when shape_id > 0 then 1 else 0 end) over (order by row_number)
            end as shape_id,
            region_size,
            region_shape_qty
        from 
            label_input_rows
    ),
    group_by_shapes as (
        select 
            row_label,
            shape_id,
            list(string_split(puzzle_input,'') order by row_number) as shape
        from 
            forward_fill
        where row_label = 'shape'
        group by row_label, shape_id
    )

    select * from group_by_shapes
    union all by name
    select row_label, region_size, region_shape_qty from forward_fill where row_label = 'region'
    
"""
input = duckdb.sql(sql_input)
# input.show()

sql_regions = """--sql
    select
        region_size,
        region_size[1]*region_size[2] as region_square_count,
        repeat(repeat('0', region_size[1]),region_size[2])::bitstring as empty_region_bitstring 
    from input 
    where row_label = 'region' 
    group by region_size 
    order by region_size
"""
regions = duckdb.sql(sql_regions)
# regions.show()

sql_shape_info = """--sql
    with count_options as (
        select shape_id, len(list_filter(flatten(shape), lambda x: x = '#')) as shape_size 
        from input
        where row_label = 'shape' 
    )

    select 
        shape_id, 
        case
            when shape_id = 3 
                then shape_size + 1 -- it's impossible to fit shape 3 together with another shape and fill the center square
            else shape_size
        end as shape_size
    from count_options
"""
shape_info = duckdb.sql(sql_shape_info)
# shape_info.show()

sql_part1a = """--sql
    with puzzles as (
        select 
            region_size,
            region_shape_qty,
            (select list(shape_size order by shape_id) from shape_info) as shape_size
        from 
            input
        where 
            row_label = 'region'
    ),
    feasability_check as (
        select 
            puzzles.region_size,
            regions.region_square_count,
            puzzles.region_shape_qty,
            list_sum(puzzles.region_shape_qty) as num_shapes,
            list_sum(list_transform(
                region_shape_qty,
                lambda x, i: x * shape_size[i]
            )) as shapes_square_count,
            shapes_square_count > region_square_count as is_definitely_not_possible
        from 
            puzzles
            left join regions
                on puzzles.region_size = regions.region_size
    ),
    easy_check as (
        select 
            region_size,
            region_shape_qty,
            is_definitely_not_possible,
            (region_size[1]//3)*(region_size[1]//3) as num_3x3_squares,
            num_shapes < num_3x3_squares as is_definitely_possible
        from 
            feasability_check
    )

    select
        region_size,
        region_shape_qty,
        is_definitely_possible,
        is_definitely_not_possible
    from easy_check
"""
part1a = duckdb.sql(sql_part1a)
# part1a.show()

sql_part1b = """--sql
    select
        case 
            when is_definitely_not_possible then 'no'
            when is_definitely_possible then 'yes'
            else 'maybe'
        end as solution_exists,
        count(*) as count_puzzles
    from part1a
    group by all
"""
part1b = duckdb.sql(sql_part1b)
part1b.show()

duckdb.sql("select sum(count_puzzles) as answer_part1 from part1b where solution_exists != 'no'").show()
# Sometimes the dumb answer is the correct answer... I tried without the mabyes first, but that didn't work
