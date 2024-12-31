This is a small script created to fix invalid sum values in the Home Assistant statistics database.
Please backup your database before executing this script, as it might contain bugs :)

Run:
python3 fix.py <db pwd> <db name> <metadata id of entry to fix>

This will recalculate the sum's in the statistics(short) tables and update the values.


BUGS: Please report them and create a PR if you have improvements!
