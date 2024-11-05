from database_connection import create_timetree_connection

# Calendar 1 is January, 2 is February, etc.
calendar = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

# Daytimes 1 is morning, 2 is forenoon, 3 is noon, 4 is afternoon, 5 is evening, 6 is night
# The start hour of each day time    1 is 6, 2 is 10, 3 is 12, 4 is 14, 5 is 17, 6 is 21
daytimes = {1: 6, 2: 10, 3: 12, 4: 14, 5: 17, 6: 21}

hours = 24


# Root Knoten der als Wurzel für den Baum dient
def add_root_node(tx):
    return tx.run(
        "CREATE (root:Root)"
    )


def add_has_year_relationship(tx, year):
    return tx.run(
        "MATCH (r:Root) "
        "CREATE (y:Year {value: $year}) "
        "CREATE (r)-[:HAS_YEAR]->(y)",
        year=year
    )


def add_has_month_relationship(tx, year, month):
    return tx.run(
        "MATCH (y:Year {value: $year}) "
        "CREATE (m:Month {value: $month}) "
        "CREATE (y)-[:HAS_MONTH]->(m)",
        year=year, month=month
    )


def add_has_month(tx, year, month):
    return tx.run(
        "MATCH (y:Year {value: $year}) "
        "CREATE (m:Month {value: $month}) "
        "CREATE (y)-[:HAS_MONTH]->(m)",
        year=year, month=month
    )


def add_has_day(tx, month, day):
    return tx.run(
        "MATCH (m:Month {value: $month}) "
        "CREATE (d:Day {value: $day}) "
        "CREATE (m)-[:HAS_DAY]->(d)",
        month=month, day=day
    )


def add_has_daytime(tx, month, day, daytime, start_hour):
    return tx.run(
        "CREATE (t:Daytime {value: $daytime, startHour: $startHour})",
        "WITH (t)"
        "MATCH(m:Month{value: $month})-[: HAS_DAY]->(d:Day{value: $day})"
        "MERGE (d)-[:HAS_DAYTIME]->(t)",
        month=month, day=day, daytime=daytime, startHour=start_hour
    )


def add_has_hour(tx, month, day, hour):
    return tx.run(
        "CREATE (h:Hour {value: $hour})"
        "WITH (h)"
        "MATCH (m:Month{value: $month})-[:HAS_DAY]->(d:Day{value: $day})"
        "MERGE (d)-[:HAS_HOUR]->(h)",
        month=month, day=day, hour=hour
    )

def create_time_tree(year):
    driver = create_timetree_connection("timeTreeOneYear")

    with driver as session:
        add_root_node(session)
        add_has_year_relationship(session, year)

        for month, days in calendar.items():
            add_has_month(session, year, month)
            for day in range(1, days + 1):
                add_has_day(session, month, day)
                for hour in range(1, hours + 1):
                    add_has_hour(session, month, day, hour)
            for day in range(1, days + 1):
                if day < days :
                    next_relationship_in_month(session, day, month)
        for month, days in calendar.items():
            if month < 12:
                next_relationship_next_month(session, days, month)


    driver.close()


def next_relationship_in_month(tx, day, month):
    return tx.run(
        "MATCH (m:Month {value: $month})-[:HAS_DAY]->(d:Day {value: $day}) "
        "WITH d, d.value AS currentDay, d.value + 1 AS nextDay, m "
        "MATCH (m)-[:HAS_DAY]->(n:Day {value: nextDay})"
        "MERGE (d)-[:NEXT]->(n)",
        day=day, month=month
    )


def next_relationship_next_month(tx, day, month):
    return tx.run(
        """
        MATCH (m:Month {value: $month})-[:HAS_DAY]->(d:Day {value: $day})
        WITH m, d, m.value AS currentMonth, m.value + 1 AS nextMonth
        MATCH (c:Month {value: nextMonth})-[:HAS_DAY]->(k:Day {value: 1})
        MERGE (d)-[:NEXT]->(k)
        """,
        day=day, month=month
    )


create_time_tree(2023)
