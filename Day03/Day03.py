import duckdb

folder = "Day03"

# input_file = f"{folder}/example.csv"
input_file = f"{folder}/input.csv"

raw_input = duckdb.read_csv(
    input_file,
    header = False,
    columns = {
        "battery_bank": "VARCHAR"
    }
)

sql_input = """--sql
    select 
        row_number() over () as battery_bank_id,
        battery_bank,
        string_split(battery_bank, '') as battery_list
    from 
        raw_input
"""

input = duckdb.sql(sql_input)
# input.show()


sql_part1 = """--sql
    with choose_digits as (
        select 
            battery_bank,
            battery_list,
            array_pop_back(battery_list) as options_first_digit,
            list_max(options_first_digit) as first_digit,
            list_position(battery_list, first_digit) as pos_first_digit,
            battery_list[(pos_first_digit + 1):] as options_second_digit,
            list_max(options_second_digit) as second_digit
        from 
            input
    ),
    joltage as (
        select 
            battery_bank,
            concat(first_digit, second_digit)::integer as joltage
        from 
            choose_digits
    )

    select sum(joltage) as answer_part1 from joltage
"""

duckdb.sql(sql_part1).show()


sql_part2a = """--sql
    with recursive choose_digits(
        battery_bank_id,
        battery_list,
        recursion_depth,
        digit,
        position_digit
    ) as (
        -- base case
            select 
                battery_bank_id as battery_bank_id,
                battery_list,
                1 as recursion_depth,
                list_max(battery_list[1:(len(battery_list) - 11)]) as digit,
                list_position(battery_list[1:], digit) as position_digit              
            from 
                input
        union all 
        -- recursive step
            select 
                dig.battery_bank_id as battery_bank_id,
                dig.battery_list as battery_list,
                dig.recursion_depth + 1 as recursion_depth,
                list_max(
                    dig.battery_list[
                        (dig.position_digit + 1):
                        (len(dig.battery_list) - 11 + dig.recursion_depth)
                    ]
                ) as digit,
                dig.position_digit + list_position(
                    dig.battery_list[(dig.position_digit + 1):],
                    list_max(
                        dig.battery_list[
                            (dig.position_digit + 1):
                            (len(dig.battery_list) - 11 + dig.recursion_depth)
                        ]
                    )
                ) as position_digit
            from
                choose_digits as dig
            where 
                dig.recursion_depth < 12 -- stop criterium
        )

        select 
            input.battery_bank_id,
            input.battery_bank,
            list(recursion_result.digit order by recursion_result.recursion_depth) as joltage_list,
            array_to_string(joltage_list, '')::bigint as joltage
        from 
            choose_digits as recursion_result
            left join input 
                on recursion_result.battery_bank_id = input.battery_bank_id
        group by all
        order by 
            input.battery_bank_id
"""
part2a = duckdb.sql(sql_part2a)

sql_part2b = """--sql
    select 
        sum(joltage) as answer_part2
    from 
        part2a
"""

duckdb.sql(sql_part2b).show()
