import duckdb

folder = "Day09"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

# use spatial extension
sql_load_spatial = """--sql
install spatial;
load spatial;
"""
duckdb.sql(sql_load_spatial)

raw_input = duckdb.read_csv(
    input_file,
    header = False,
    columns = {
        "x": "integer",
        "y": "integer"
    }
)
# raw_input.show()

sql_input = """--sql
    select
        row_number() over () as tile_id,
        x,
        y
    from 
        raw_input
"""
input = duckdb.sql(sql_input)
# input.show()


sql_part1 = """--sql
    with rectangles as (
        select
            corner1.tile_id as tile1_id1,
            [corner1.x, corner1.y] as tile1_xy,
            corner2.tile_id as tile_id2,
            [corner2.x, corner2.y] as tile2_xy,
            (abs(corner1.x - corner2.x) + 1)::bigint * (abs(corner1.y - corner2.y) + 1)::bigint as area
        from 
            input as corner1
            inner join input as corner2
                on (
                    [corner1.x, corner1.y] != [corner2.x, corner2.y]
                )
    )

    select area as answer_part1 from rectangles order by area desc limit 1
"""
part1 = duckdb.sql(sql_part1)
part1.show()


sql_part2 = """--sql
    with rectangles as (
        select
            printf('polygon ((%1$d %2$d, %1$d %4$d, %3$d %4$d, %3$d %2$d, %1$d %2$d))', corner1.x, corner1.y, corner2.x, corner2.y)::geometry as rectangle,
            (abs(corner1.x - corner2.x) + 1)::bigint * (abs(corner1.y - corner2.y) + 1)::bigint as area
        from 
            input as corner1
            inner join input as corner2
                on [corner1.x, corner1.y] != [corner2.x, corner2.y]
    ),
    red_green_tiles as (
        select 
           ('polygon ((' || string_agg( printf('%d %d', x, y), ', ' order by tile_id) || '))')::geometry as good_floor
        from 
            (
                select * from input 
                union all 
                select tile_id + 10000 as tile_id, x, y from input where tile_id = 1
            )
    ),
    red_green_rectangles as (
        select 
            rectangle,
            area
        from 
            rectangles
            inner join red_green_tiles
                on (
                    ST_Within(rectangle, good_floor) -- ture if first geometry is within second
                )
    )

    select area as answer_part2 from red_green_rectangles order by area desc limit 1
"""
duckdb.sql(sql_part2).show()
