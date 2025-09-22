This is a small script created to fix invalid sum values in the Home Assistant statistics database.
This will recalculate the sum's in the statistics(short) tables and update the values.
Please backup your database before executing this script, as it might contain bugs :)

# WARNING!!!
Read and follow these steps:
1. Stop the HomeAssistant. Use HomeAssistant Terminal, or host command line and execute command 'ha core stop'.
2. Backup your HomeAssistant database, really, really, really, pretty please.
   - SQLite:
     -  You can use HomeAssistant Backup functions (in this case do this before stopping HA)
     -  or copy SQLite DB file home_assistant_v2.db somewhere safe
   - MYSQL:
     - Backup HA MYSQL database or at least tables 'statistics' and 'statistics_short_term' using mysqldump or other appropriate tool.
3. Correct any erroneous recorder data (typically 'spikes' anomalies) in the HomeAssistant
   database tables 'statistics_short_term' and/or 'statistics' in the column 'state'.
4. Execute this tool, see options bellow.
5. Restart the HomeAssistant. Use HomeAssistant host command line and execute command 'ha core start' or restart host."

# USAGE:

python3 fix.py -h | -d|--database DB (-e|--entities_names ENT)|(-i|--entities_ids ENT_ID)(|-a|--all)(-p|--preview)

| short arg | long arg         | explanation                                                                                     |
| --------- | ---------------- | ----------------------------------------------------------------------------------------------- |
| -h        | --help           | print this help                                                                                 |
| -d        | --database       | path to SQLite db file or MYSQL connect string: user:password@ip:database                       |
| -e        | --entities_names | comma separated list of the recorder entities names to correct, typically sensors entities      |
| -i        | --entities_ids   | comma separated list of the recorder entities IDs to correct (id from the statistic_meta table) |
| -a        | --all            | correct records for all recorder entities                                                       |
| -p        | --preview        | only preview invalid records, do not change any data                                            |


# BUGS:
Please report them and create a PR if you have improvements!
