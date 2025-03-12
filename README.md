This is a small script created to fix invalid sum values in the Home Assistant statistics database.
This will recalculate the sum's in the statistics(short) tables and update the values.
Please backup your database before executing this script, as it might contain bugs :)

WARNING!!!
Read and follow these steps:
1. Stop the HomeAssistant.
2. Backup your HomeAssistant database, really, really, really, pretty please.
3. Correct any erroneous recorder data (typically 'spikes' anomalies) in the HomeAssistant
   database tables 'statistics_short_term' and/or 'statistics' in the column 'state'.
4. Execute this tool, see options bellow.
5. Restart the HomeAssistant.

USAGE:
# python3 fix.py -h | -d|--database DB (-e|--entities_names ENT)|(-i|--entities_ids ENT_ID)(|-a|--all)
-h	--help			print this help
-d	--database		path to SQLite db file or MYSQL connect string: user:password@ip:database
-e	--entities_names 	comma separated list of the recorder entities names to correct, typically sensors entities
-i	--entities_ids 		comma separated list of the recorder entities IDs to correct (id from the statistic_meta table)
-a	--all			correct records for all recorder entities


BUGS: Please report them and create a PR if you have improvements!
