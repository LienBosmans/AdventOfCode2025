import duckdb

folder = "Day10"

input_file = f"{folder}/example.csv"
# input_file = f"{folder}/input.csv"

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
input.show()

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
