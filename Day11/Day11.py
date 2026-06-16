import duckdb

folder = "Day11"

# input_file = f"{folder}/example_part1.csv"
# input_file = f"{folder}/example_part2.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    sep = "🪿", # use dummy sep so everything is ingested in one column
    header = False,
    columns = {
        "device_input": "VARCHAR"
    }
)

sql_input = """--sql
    with from_to_list as (
        select 
            string_split(device_input, ': ')[1] as from_id,
            string_split(device_input, ': ')[2] as to_ids
        from 
            raw_input
    )

    select
        from_id,
        unnest(string_split(to_ids, ' ')) as to_id
    from
        from_to_list
"""
input = duckdb.sql(sql_input)
# input.show()


sql_part1 = """--sql
    with recursive find_paths (depth, node_id, visited_nodes)
    as (
        -- anchor
        select 
            0 as depth,
            to_id as node_id,
            [from_id, to_id] as visited_nodes
        from 
            input
        where 
            from_id = 'you'

        union all
        
        -- recursion
        select
            find_paths.depth + 1 as depth,
            input.to_id as node_id,
            list_append(visited_nodes, input.to_id) as visited_nodes
        from 
            find_paths
            inner join input
                on (
                    find_paths.node_id = input.from_id
                    and input.to_id not in visited_nodes -- avoid loops
                ) 
        where
            depth < 100
    )

    select count(*) as answer_part1 from find_paths where node_id = 'out'
"""
part1 = duckdb.sql(sql_part1)
part1.show()


sql_part2a = """--sql
    with recursive topological_sort(node, sort_order) using key (node)
    as (
        -- anchor
        select
            from_id as node,
            row_number() over () as sort_order
        from 
            input
            anti join input as pred
                on input.from_id = pred.to_id
        group by 
            from_id

        union all

        -- recursion
        select 
            to_id as node,
            (select max(sort_order) from topological_sort) +  row_number() over () as sort_order
        from 
            input
        where 
            to_id not in (select node from recurring.topological_sort where sort_order > 0)
        group by 
            input.to_id
        having
            list_has_all((select list(node) from recurring.topological_sort where sort_order > 0), list(input.from_id))
    )

    select * from topological_sort
"""
part2a = duckdb.sql(sql_part2a)
# part2a.show()

paths = []
for start_node, end_node in [['svr','fft'], ['fft','dac'], ['dac','out']]:
# I could do this in pure SQL, but that would mean copy-pasting this code block 3 times, only changing the start & end, which is stupid.
    sql_part2b = """--sql
        with recursive count_paths (node, node_order, num_paths_to_node) using key (node)
        as (
            -- anchor
            select 
                ts.node as node,
                ts.sort_order as node_order,
                1::bigint as num_paths_to_node
            from 
                part2a as ts
            where ts.node = '""" + start_node + """'--sql

            union all 

            select
                ts.node as node,
                ts.sort_order as node_order,
                sum(count_paths.num_paths_to_node) as num_paths_to_node
            from 
                part2a as ts
                left join input
                    on ts.node = input.to_id
                left join recurring.count_paths
                    on input.from_id = count_paths.node
            where 
                ts.sort_order = (select max(node_order) + 1 from count_paths)
            group by 
                ts.node, ts.sort_order
        )

        select num_paths_to_node from count_paths where node = '""" + end_node + """'--sql
    """
    part2b = duckdb.sql(sql_part2b)
    # print(start_node, 'to', end_node, ':', part2b.fetchall()[0][0])
    paths.append(part2b.fetchall()[0][0])

duckdb.sql(f"select ({paths[0]}::bigint * {paths[1]}::bigint * {paths[2]})::bigint as answert_part2").show()
