import sys
import re
import getopt
from pathlib import Path


# Function for connecting to MySQL database
def connect_db():
    # Trying to connect
    try:
        if db == "sqlite":
            db_connection = sqlite3.connect(database)
            db_connection.row_factory = sqlite3.Row
        elif db == "mysql":
            db_connection = MySQLdb.connect(
                database[3], database[1], database[2], database[4]
            )
    # If connection is not successful
    except Exception as e:
        print(f"An error occurred connecting to DB: {e}")
        exit(1)
    # If Connection Is Successful
    print("Connected")
    return db_connection


def fix_record(db_conn3, id, new_sum):
    cursor = db_conn3.cursor()
    qry = "UPDATE statistics SET sum = {} WHERE id = {}".format(new_sum, id)
    # print(qry)
    cursor.execute(qry)


def fix_record_short(db_conn3, id, new_sum):
    cursor = db_conn3.cursor()
    qry = "UPDATE statistics_short_term SET sum = {} WHERE id = {}".format(new_sum, id)
    # print(qry)
    cursor.execute(qry)


def help(error):
    if error:
        print("\nERROR: " + error)
    print("\nWARNING!!!")
    print("Read and follow these steps:")
    print("1. Stop the HomeAssistant.")
    print(
        "2. Backup your HomeAssistant database, really, really, really, pretty please."
    )
    print(
        "3. Correct any erroneous recorder data (typically 'spikes' anomalies) in the HomeAssistant"
    )
    print(
        "   database tables 'statistics_short_term' and/or 'statistics' in the column 'state'."
    )
    print("4. Execute this tool, see options bellow.")
    print("5. Restart the HomeAssistant.")
    print("\nUSAGE:")
    print(
        "# python3 fix.py -h | -d|--database DB (-e|--entities_names ENT)|(-i|--entities_ids ENT_ID)(|-a|--all)"
    )
    print("-h\t--help\t\t\tprint this help")
    print(
        "-d\t--database\t\tpath to SQLite db file or MYSQL connect string: user:password@ip:database"
    )
    print(
        "-e\t--entities_names \tcomma separated list of the recorder entities names to correct, typically sensors entities"
    )
    print(
        "-i\t--entities_ids \t\tcomma separated list of the recorder entities IDs to correct (id from the statistic_meta table)"
    )
    print("-a\t--all\t\t\tcorrect records for all recorder entities\n")
    if error:
        exit(1)
    exit(0)


# Remove 1st argument from the
# list of command line arguments
argumentList = sys.argv[1:]

# Options
options = "hd:e:i:a"

# Long options
long_options = ["help", "database=", "entities_names=", "entities_ids=", "all"]

entities_all = None
entities_ids = None
entities_names = None

try:
    # Parsing argument
    arguments, values = getopt.getopt(argumentList, options, long_options)

    # checking each argument
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-h", "--help"):
            help(None)
        elif currentArgument in ("-d", "--database"):
            database = currentValue
        elif currentArgument in ("-e", "--entities_names"):
            entities_names = currentValue
        elif currentArgument in ("-i", "--entities_ids"):
            entities_ids = currentValue
        elif currentArgument in ("-a", "--all"):
            entities_all = 1

except getopt.error as err:
    # output error, and return with an error code
    help("Invalid input: " + str(err))

if database is None:
    help("Database files must be specified!")
elif match := re.match("([^\:]+)\:([^\@])+\@([^\:]+)\:(.+)", database):
    import MySQLdb
    import MySQLdb.cursors

    db = "mysql"
    database = [match.group(1), match.group(2), match.group(3), match.group(4)]
elif Path(database).is_file():
    import sqlite3

    db = "sqlite"
else:
    help("Invalid database parameter")

if entities_names:
    entities_names = list(map(str.strip, entities_names.split(",")))
    if not len(entities_names) or not (
        all([re.match("\w+\.\w+", val) for val in entities_names])
    ):
        help("Entities shall be comma separated list of entities names")
elif entities_ids:
    entities_ids = list(map(str.strip, entities_ids.split(",")))
    if not len(entities_ids) or not (
        all([isinstance(val, int) for val in entities_ids])
    ):
        help("Entities shall be comma separated list of entities IDs")
elif entities_all:
    print("Executing for all recorder entities")
else:
    help("No entities to repair were specified")


# Function Call For Connecting To Our Database
db_conn1 = connect_db()
db_conn2 = connect_db()
db_conn3 = connect_db()

# Making Cursor Object For Query Execution
if db == "sqlite":
    cursor = db_conn1.cursor()
elif db == "mysql":
    cursor = db_conn1.cursor(MySQLdb.cursors.DictCursor)

# Executing Query
if entities_names:
    cursor.execute(
        "SELECT * FROM statistics_meta WHERE has_sum = 1 AND statistic_id IN ({})".format(
            result=", ".join(entities_names)
        )
    )
if entities_ids:
    cursor.execute(
        "SELECT * FROM statistics_meta WHERE has_sum = 1 AND id IN ({});".format(
            result=", ".join(entities_ids)
        )
    )
else:
    cursor.execute("SELECT * FROM statistics_meta WHERE has_sum = 1")

# Above Query Gives Us The Current Date
# Fetching Data
records = cursor.fetchall()

for row in records:
    row = dict(row)
    prev_sum = None
    prev_state = None
    last = {"id": None, "start": None, "state": None, "sum": None}

    print("- Checking Entity: {} Id: {}".format(row["statistic_id"], row["id"]))
    cursor2 = db_conn2.cursor()
    qry = "SELECT * FROM statistics WHERE metadata_id = {} ORDER BY start_ts".format(
        row["id"]
    )
    cursor2.execute(qry)
    stats = cursor2.fetchall()
    for stat in stats:
        stat = dict(stat)
        if stat["state"] is None:
            # skip rows with NULL state values
            continue
        elif prev_state is None:
            # set initial values
            prev_state = stat["state"]
            prev_sum = stat["sum"]
        elif stat["state"] > prev_state:
            prev_sum = prev_sum + (stat["state"] - prev_state)
        elif stat["state"] < prev_state:
            prev_sum = prev_sum + stat["state"]

        if prev_sum != stat["sum"]:
            print(
                "-  Statistics Record: {} has invalid sum: {}, shall be: {}".format(
                    stat["id"], stat["sum"], prev_sum
                )
            )
            fix_record(db_conn3, stat["id"], prev_sum)

        prev_state = stat["state"]
        last["id"] = stat["id"]
        last["sum"] = prev_sum
        last["state"] = stat["state"]
        last["start"] = stat["start_ts"]

    db_conn3.commit()

    if last["start"] is None or last["sum"] is None:
        print("ERROR: no valid Statistic Record was found")
        continue

    if db == "sqlite":
        cursor = db_conn2.cursor()
    elif db == "mysql":
        cursor = db_conn2.cursor(MySQLdb.cursors.DictCursor)

    if last["start"] is None or last["sum"] is None:
        print("ERROR: no valid Statistic Record was found")
        continue

    # Get last known value from short_temp
    qry = "SELECT * FROM statistics_short_term WHERE metadata_id = {} ORDER BY start_ts".format(
        row["id"]
    )
    cursor2.execute(qry)
    stats = cursor2.fetchall()

    prev_sum = None
    prev_state = None
    for stat in stats:
        stat = dict(stat)
        if stat["state"] is None:
            # skip rows with NULL state values
            continue
        elif stat["start_ts"] == last["start"]:
            # first row to start checking, initialize values
            prev_sum = last["sum"]
            prev_state = stat["state"]
            if stat["state"] != last["state"]:
                print(
                    "-  Short Statistic record: {} has different state value: {} than Statistic record: {} state value: {}".format(
                        stat["id"], stat["state"], last["id"], last["state"]
                    )
                )
                # exit(1)
        elif prev_state is None:
            # skip rows before last_start
            continue
        if stat["state"] > prev_state:
            # calculate expected correct sum value
            prev_sum = prev_sum + (stat["state"] - prev_state)
        elif stat["state"] < prev_state:
            # calculate expected correct sum value
            prev_sum = prev_sum + stat["state"]

        if prev_sum != stat["sum"]:
            # correct sum if different than expected
            print(
                "-  Short Statistics Record: {} has invalid sum: {}, shall be: {}".format(
                    stat["id"], stat["sum"], prev_sum
                )
            )
            fix_record_short(db_conn3, stat["id"], prev_sum)

        prev_state = stat["state"]
    if prev_state is None:
        print(
            "ERROR: no Short Statistic Record was found for start_ts: {}".format(
                last["start"]
            )
        )

    next_sum = None
    next_state = None
    for stat in reversed(stats):
        stat = dict(stat)
        if stat["state"] is None:
            # skip rows with NULL state values
            continue
        elif stat["start_ts"] == last["start"]:
            # first row to start checking, initialize values
            next_sum = last["sum"]
            next_state = stat["state"]
            if stat["state"] != last["state"]:
                print(
                    "-  Short Statistic record: {} has different state value: {} than Statistic record: {} state value: {}".format(
                        stat["id"], stat["state"], last["id"], last["state"]
                    )
                )
                # exit(1)
        elif next_state is None:
            # skip rows after last_start
            continue
        elif stat["state"] < next_state:
            # calculate expected correct sum value
            next_sum = next_sum - (next_state - stat["state"])
        elif stat["state"] > next_state:
            # calculate expected correct sum value
            next_sum = next_sum - next_state

        if next_sum != stat["sum"]:
            # correct sum if different than expected
            print(
                "-  Short Statistics record: {} has invalid sum: {}, shall be: {}".format(
                    stat["id"], stat["sum"], next_sum
                )
            )
            fix_record_short(db_conn3, stat["id"], next_sum)

        next_state = stat["state"]
    if next_state is None:
        print(
            "ERROR: no Short Statistic Record was found for start_ts: {}".format(
                last["start"]
            )
        )

    db_conn3.commit()

db_conn1.close()
db_conn2.close()
db_conn3.close()
print("Disconnected")
