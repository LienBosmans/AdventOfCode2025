import duckdb

folder = "Day10"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    sep = "🪿", # use dummy sep so everything is ingested in one column
    header = False,
    columns = {
        "machine_input": "VARCHAR"
    }
)

sql_input = """--sql
    with add_machine_id as (
        select 
            row_number() over () as machine_id,
            unnest(string_split(machine_input, ' ')) as machine_input
        from 
            raw_input
    ),
    add_labels as (
        select 
            machine_id,
            machine_input[2:len(machine_input)-1] as machine_input,
            case
                when contains(machine_input, '[') then 'light_diagram'
                when contains(machine_input, '(') then 'button_wiring'
                when contains(machine_input, '{') then 'joltage_requirement'
                else 'unknown'
            end as label
        from 
            add_machine_id
    ),
    recombine as (
        select 
            light_diagram.machine_id,
            light_diagram.machine_input as light_diagram,
            replace(replace(light_diagram.machine_input, '.', '0'), '#', '1')::bitstring as light_diagram_bitstring,
            row_number() over (partition by light_diagram.machine_id) as button_id,
            string_split(button.machine_input, ',') as button,
            array_to_string(list_transform(
                range(0, len(light_diagram.machine_input)),
                lambda x: (x::varchar in string_split(button.machine_input, ','))::integer
            ),'')::bitstring as button_bitstring,
            string_split(joltage_requirement.machine_input, ',') as joltage_requirement,
        from
            (select machine_id, machine_input from add_labels where label = 'light_diagram') as light_diagram
            inner join (select machine_id, machine_input from add_labels where label = 'joltage_requirement') as joltage_requirement
                on light_diagram.machine_id = joltage_requirement.machine_id
            inner join (select machine_id, machine_input from add_labels where label = 'button_wiring') as button
                on light_diagram.machine_id = button.machine_id
    )

    select * from recombine
"""
input = duckdb.sql(sql_input)
# input.show()

sql_part1a = """--sql
    with recursive press_buttons(depth, machine_id, solution, light_state, pressed_buttons, is_solved)
    as (
        -- anchor
        select
            0 as depth,
            machine_id,
            light_diagram_bitstring as solution,
            repeat('0', len(light_diagram))::bitstring as light_state,
            [] as pressed_buttons,
            ( light_diagram_bitstring = repeat('0', len(light_diagram))::bitstring )::boolean as is_solved
        from
            input
        group by all

        union all

        -- recursion
        select
            press_buttons.depth + 1 as depth, 
            press_buttons.machine_id,
            press_buttons.solution,
            xor(press_buttons.light_state, input.button_bitstring) as light_state,
            list_append(press_buttons.pressed_buttons, input.button_id) as pressed_buttons,
            ( press_buttons.solution = xor(press_buttons.light_state, input.button_bitstring) )::boolean as is_solved
        from 
            press_buttons
            left join input
                on (
                    press_buttons.machine_id = input.machine_id
                    and not input.button_id in press_buttons.pressed_buttons
                )
        where 
            depth < 200 -- saftely stop
            and press_buttons.machine_id in -- only continue pressing buttons if solution not yet found
                (
                    select machine_id from press_buttons group by machine_id having not bool_or(is_solved)
                )
    )

    select * from press_buttons
"""
part1a = duckdb.sql(sql_part1a)
# part1a.show()

sql_part1b = """--sql 
    with press_count as (
        select machine_id, max(depth) as fewest_presses from part1a group by machine_id
    )

    select sum(fewest_presses) as answer_part1 from press_count
"""
part1b = duckdb.sql(sql_part1b)
part1b.show()

sql_part2a = """--sql
    with recursive possible_button_presses (depth, machine_id, pressed_buttons, last_button_id, light_state, joltage_state)
    as (
        -- anchor
        select
            0 as depth,
            machine_id,
            [] as pressed_buttons,
            0 as last_button_id,
            repeat('0', len(light_diagram))::bitstring as light_state,
            repeat([0], len(light_diagram)) as joltage_state
        from 
            input

        union all

        -- recursion
        select 
            bp.depth + 1 as depth,
            bp.machine_id as machine_id,
            list_append(bp.pressed_buttons, input.button_id) as pressed_buttons,
            input.button_id as last_button_id,
            xor(bp.light_state, input.button_bitstring) as light_state,
            list_transform(
                bp.joltage_state, 
                lambda x, i: x + (case when (i-1)::varchar in button then 1 else 0 end) 
            ) as joltage_state
        from 
            possible_button_presses as bp
            inner join input
                on (
                    bp.machine_id = input.machine_id
                    and bp.last_button_id < input.button_id -- press each button maximum one time, order doesn't matter
                )
        where 
            depth < 500 -- safety stop (inner join is real stop)
    )

    select
        machine_id,
        light_state,
        joltage_state,
        depth as number_of_presses,
        pressed_buttons
    from possible_button_presses
    -- where depth > 0
    group by all
    order by machine_id, light_state
"""
part2a = duckdb.sql(sql_part2a)
# part2a.show()

sql_part2b = """--sql
    with recursive press_buttons_smartly (depth, machine_id, pressed_buttons, og_joltage_req, joltage_requirement, joltage_state, light_diagram, button_presses)
    as (
        -- anchor
        select 
            0 as depth,
            machine_id,
            [] as pressed_buttons,
            joltage_requirement::integer[] as og_joltage_req,
            joltage_requirement::integer[] as joltage_requirement,
            repeat([0], len(light_diagram)) as joltage_state,
            array_to_string(list_transform(
                joltage_requirement::integer[],
                lambda x: x % 2
            ), '')::bitstring as light_diagram,
            0 as button_presses
        from 
            input
        group by all

        union all 

        -- recursion
        select 
            pb.depth + 1 as depth,
            pb.machine_id as machine_id,
            concat(pb.pressed_buttons, [-pb.depth - 1], possible_buttons.pressed_buttons) as pressed_buttons,
            pb.og_joltage_req as og_joltage_req,
            list_transform(
                pb.joltage_requirement,
                lambda x, i: ( x - possible_buttons.joltage_state[i] )//2
            ) as joltage_requirement,
            list_transform(
                    pb.joltage_state, 
                    lambda x, i: x + (2**pb.depth) * possible_buttons.joltage_state[i]
            ) as joltage_state,
            array_to_string(list_transform(
                joltage_requirement,
                lambda x, i: (( x - possible_buttons.joltage_state[i] )//2 % 2)::integer
            ), '')::bitstring as light_diagram,
            pb.button_presses + (2**pb.depth) * possible_buttons.number_of_presses as button_presses
        from 
            press_buttons_smartly as pb
            inner join part2a as possible_buttons
                on (
                    pb.machine_id = possible_buttons.machine_id
                    and pb.light_diagram = possible_buttons.light_state
                    and list_min( list_transform(
                            pb.og_joltage_req,
                            lambda x, i: x - pb.joltage_state[i] - (2**pb.depth) * possible_buttons.joltage_state[i]
                        ) ) >= 0
                )
        where 
            depth < 100
    )

    select * from press_buttons_smartly order by machine_id, depth

"""
part2b = duckdb.sql(sql_part2b)
# part2b.show()

sql_part2c = """--sql
    with press_count as (
        select 
            machine_id,
            min(button_presses) as fewest_presses,
        from 
            part2b
        where 
            list_max(joltage_requirement) = 0
        group by machine_id
    )

    select sum(fewest_presses) as answer_part2 from press_count
"""
part2c = duckdb.sql(sql_part2c)
part2c.show()
