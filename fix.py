# Module For Connecting To MySQL database
import sys
import MySQLdb
import MySQLdb.cursors

# Function for connecting to MySQL database
def connect():
    # Trying to connect
    try:
        db_connection = MySQLdb.connect("localhost", "root", sys.argv[1], sys.argv[2])
    # If connection is not successful
    except MySQLdb.Error:
        print("Can't connect to database")
        return 0
    # If Connection Is Successful
    print("Connected")

    return db_connection


def fix_record(dbconn3, id, new_sum):
    cursor = dbconn3.cursor()
    cursor.execute("UPDATE statistics SET sum = %s WHERE id = %s", (new_sum, id))

def fix_record_short(dbconn3, id, new_sum):
    cursor = dbconn3.cursor()
    cursor.execute("UPDATE statistics_short_term SET sum = %s WHERE id = %s", (new_sum, id))


# Function Call For Connecting To Our Database
dbconn1 = connect()
dbconn2 = connect()
dbconn3 = connect()

# Making Cursor Object For Query Execution
cursor = dbconn1.cursor(MySQLdb.cursors.DictCursor)

# Executing Query
cursor.execute("SELECT * FROM statistics_meta WHERE has_sum = 1 AND id = {};".format(sys.argv[3]))

# Above Query Gives Us The Current Date
# Fetching Data
records = cursor.fetchall()

for row in records:
    last_sum = 0.0
    prev_sum = 0.0
    prev_corrected_sum = 0.0
    prev_state = 0.0
    correction = 0.0
    last_start = 0
    print("To Fix: ", row['statistic_id'])
    cursor2 = dbconn2.cursor(MySQLdb.cursors.DictCursor)
    qry = "SELECT * FROM statistics WHERE metadata_id = {} ORDER BY start".format(row['id'])
    cursor2.execute(qry)
    stats = cursor2.fetchall()
    for stat in stats:
        # Previous sum was bigger, so there was a reset
        if stat['sum'] < prev_sum:
            print("Prev SUM {} and current sum {}".format(prev_sum, stat['sum']))
            # Last sum should contain sum before the reset + previous sum
            # As the new sums should be calculated against the previous sum
            last_sum = last_sum + prev_sum + correction
            # As the current sum doesn't include all data, we need to use prev state to calculate the sum
            sum = (stat['state'] - prev_state) + last_sum
            # As for the next calculations we use the reset sum, we need to take the correction with us
            correction = (stat['state'] - prev_state) - stat['sum']
            print("Stat {} was reset at {}".format(row['id'], stat['state']))
            print("Sum was {} and should be {} ({} {} {})".format(stat['sum'], sum, last_sum, prev_sum, correction))
            prev_sum = stat['sum']
            prev_corrected_sum = sum
            fix_record(dbconn3, stat['id'], sum)
        elif stat['sum'] < prev_corrected_sum:
            sum = stat['sum'] + last_sum + correction
            print("Sum2 was {} and should be {} ({} {} {})".format(stat['sum'], sum, last_sum, prev_sum, correction))
            fix_record(dbconn3, stat['id'], sum)
            prev_sum = stat['sum']
            prev_corrected_sum = sum
            prev_state = stat['state']
        else:
            prev_sum = stat['sum']
            prev_state = stat['state']
        last_start = stat['start']

    dbconn3.commit()

    cursor2 = dbconn2.cursor(MySQLdb.cursors.DictCursor)

    # Get last known value from short_temp
    qry = "SELECT * FROM statistics_short_term WHERE metadata_id = {} ORDER BY start".format(row['id'])
    cursor2.execute(qry)
    stats = cursor2.fetchall()
    id = 0
    prev_sum_invalid = 0.0
    # Save the last SUM from statistics as we will use this value as a base
    prev_sum_base = prev_corrected_sum
    for stat in stats:
        if stat['start'] == last_start:
            print("Found base entry with id {}".format(stat['id']))
            id = stat['id']
            prev_sum_invalid = stat['sum']
            fix_record_short(dbconn3, id, prev_sum_base)
        elif id != 0:
            prev_sum = prev_sum + (stat['sum'] - prev_sum_invalid)
            prev_sum_invalid = stat['sum']
            fix_record_short(dbconn3, stat['id'], prev_sum)

    id = 0
    prev_sum_invalid = 0.0
    # Our starting point is again the good sum value from statistics table
    prev_sum = prev_sum_base
    # We loop backwards to fix older data
    for stat in reversed(stats):
        if stat['start'] == last_start:
            print("Found base entry with id {}".format(stat['id']))
            id = stat['id']
            prev_sum_invalid = stat['sum']
            prev_state = stat['state']
            fix_record_short(dbconn3, id, prev_sum_base)
        elif id != 0:
            # There was another reset here, as current sum
            # Fallback to state calculation then
            if (prev_sum_invalid - stat['sum']) < 0:
                prev_sum = prev_sum - (prev_state - stat['state'])
            else:
                prev_sum = prev_sum - (prev_sum_invalid - stat['sum'])
            prev_sum_invalid = stat['sum']
            prev_state = stat['state']
            fix_record_short(dbconn3, stat['id'], prev_sum)

dbconn3.commit()

