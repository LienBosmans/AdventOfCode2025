import duckdb

folder = "Day08"

# input_file = f"{folder}/example.csv"; num_connections = 10; num_boxes = 20
input_file = f"{folder}/input.csv"; num_connections = 1000; num_boxes = 1000

raw_input = duckdb.read_csv(
    input_file,
    header = False,
    columns = {
        "xpos": "INTEGER",
        "ypos": "INTEGER",
        "zpos": "INTEGER"
    }
)
# raw_input.show()

sql_input = """--sql
    select 
        row_number() over () as box_id,
        xpos,
        ypos,
        zpos
    from 
        raw_input
"""
input = duckdb.sql(sql_input)

sql_all_connections = """--sql
    with connections as (
        select 
            list_max([left_box.box_id, right_box.box_id]) as from_box_id,
            list_min([left_box.box_id, right_box.box_id]) as to_box_id,
            list_distance(
                [left_box.xpos, left_box.ypos, left_box.zpos],
                [right_box.xpos, right_box.ypos, right_box.zpos]
            ) as distance
        from
            input as left_box
            inner join input as right_box
                on left_box.box_id < right_box.box_id
    )   
    select from_box_id, to_box_id, distance, row_number() over (order by distance) as c_order from connections order by distance
"""
all_connections = duckdb.sql(sql_all_connections)
all_connections.to_csv("all_connections.csv")
# all_connections.show()

sql_connections = """---sql
    with filtered_connections as (
         select from_box_id, to_box_id, distance
         from all_connections 
         order by distance 
         limit """ + str(num_connections) + """--sql
    )

    select from_box_id, to_box_id, distance, row_number() over () as c_order from filtered_connections order by distance
"""
connections = duckdb.sql(sql_connections)
# connections.show()

sql_part1a = """--sql
    with recursive grow_trees(tree, depth)
    as (
        -- anchor
        select
            [connections.from_box_id, connections.to_box_id] as tree,
            0 as depth
        from 
            connections

        union 

        -- recursive step      
        select
            list_sort(list_distinct(flatten(list(concat(
                left_trees.tree,
                right_trees.tree
            ))))) as tree,
            left_trees.depth + 1 as depth
        from
            grow_trees as left_trees
            left join grow_trees as right_trees
                on list_has_any(left_trees.tree, right_trees.tree)
        where 
            left_trees.depth < 100 -- safety stop
            and exists (
                from grow_trees as left_trees
                inner join grow_trees as right_trees
                    on (
                        list_has_any(left_trees.tree, right_trees.tree)
                        and not left_trees.tree = right_trees.tree
                    )
            )
        group by 
            left_trees.depth,
            left_trees.tree
    )

    select * from grow_trees order by depth, len(tree)
"""
part1a = duckdb.sql(sql_part1a)
# part1a.show()

# duckdb.sql("select max(depth) from part1a").show()

sql_part1b = """--sql
    with circuits_top3 as (
        select
            tree as circuit_members,
            len(tree) as circuit_size
        from 
            part1a
        where
            depth = (select max(depth) from part1a)
        group by all
        order by circuit_size desc
        limit 3
    )

    select product(circuit_size)::integer as answer_part1 from circuits_top3
"""
part1b = duckdb.sql(sql_part1b)
part1b.show()


sql_part2a = """--sql
    with recursive grow_trees(tree, depth)
    as (
        -- anchor
        select
            [all_connections.from_box_id, all_connections.to_box_id] as tree,
            1 as depth
        from 
            all_connections
        where
            c_order = 1

        union 

        -- recursive step      
        select
            list_sort(list_distinct(flatten(list(concat(left_trees.tree, right_trees.tree))))) as tree,
            left_trees.depth + 1 as depth
        from
            (
                select [from_box_id, to_box_id] as tree, (select max(depth) from grow_trees) as depth from all_connections where c_order = (select max(depth) from grow_trees) + 1
                union all select tree, depth from grow_trees
            ) as left_trees
            left join grow_trees as right_trees
                on list_has_any(left_trees.tree, right_trees.tree)
        where 
            left_trees.depth < 10000 -- safety stop
            and (select max(len(tree)) from grow_trees) < """ + str(num_boxes) + """--sql
        group by 
            left_trees.depth,
            left_trees.tree
    )

    select * from grow_trees order by depth, len(tree)
"""
part2a = duckdb.sql(sql_part2a)
# part2a.show()

sql_part2b = """--sql
    select product(input.xpos)::integer as answer_part2
    from 
        input inner join all_connections
            on input.box_id in [all_connections.from_box_id, all_connections.to_box_id]
    where
        all_connections.c_order = (select max(depth) from part2a)
"""
part2b = duckdb.sql(sql_part2b)
part2b.show()
