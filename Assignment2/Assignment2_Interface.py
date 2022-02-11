#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading


# PARALLEL SORT
def sort_partition(InputTable, SortingColumnName, table_prefix, index, lower, upper, openconnection):
    cursor = openconnection.cursor()

    table_name = table_prefix + str(index)

    drop_query = "DROP TABLE IF EXISTS {}"
    create_query = "CREATE TABLE {} (LIKE {} INCLUDING ALL)"

    # Create the partition table
    cursor.execute(drop_query.format(table_name))
    cursor.execute(create_query.format(table_name, InputTable))

    # For the first partition table, the range is [0, lower]
    insert_query_first = " \
                INSERT INTO {} \
                SELECT * FROM " + InputTable + " \
                WHERE " \
                         + SortingColumnName + " >= " + str(lower) \
                         + " AND " + SortingColumnName + " <= " + str(upper) \
                         + " ORDER BY " + SortingColumnName + " ASC"

    # For rest of the partition tables, the range is (lower, upper]
    insert_query = " \
                INSERT INTO {} \
                SELECT * FROM " + InputTable + " \
                WHERE " \
                   + SortingColumnName + " > " + str(lower) \
                   + " AND " + SortingColumnName + " <= " + str(upper) \
                   + " ORDER BY " + SortingColumnName + " ASC"

    if index == 0:
        cursor.execute(insert_query_first.format(table_name))

    else:
        cursor.execute(insert_query.format(table_name))


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    cursor = openconnection.cursor()
    cursor.execute("CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable + " WHERE 0=1 ;")

    # Find the range
    cursor.execute("SELECT MIN( " + SortingColumnName + ") FROM " + InputTable)
    min_val = cursor.fetchone()[0]
    cursor.execute("SELECT MAX( " + SortingColumnName + ") FROM " + InputTable)
    max_val = cursor.fetchone()[0]
    range_val = float(max_val - min_val) / 5

    thread = [0] * 5
    table_prefix = "range_partition"

    # create partition tables and sort each table
    for index in range(0, 5):
        lower = min_val + index * range_val
        upper = lower + range_val
        thread[index] = threading.Thread(
            target=sort_partition,
            args=(InputTable, SortingColumnName, table_prefix, index, lower, upper, openconnection)
        )
        thread[index].start()

    # Insert all sorted values into the output table
    for index in range(0, 5):
        thread[index].join()
        insert_query = "INSERT INTO {} SELECT * FROM {}"
        cursor.execute(insert_query.format(OutputTable, table_prefix + str(index)))

    openconnection.commit()


# PARALLEL JOIN
def join_partitions(InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, table_prefix, output_part_prefix, index, lower,
          upper, openconnection):
    cursor = openconnection.cursor()

    drop_query = "DROP TABLE IF EXISTS {}"
    create_query = "CREATE TABLE {} (LIKE {} INCLUDING ALL)"

    table1_part_name = "table1_" + table_prefix + str(index)
    table2_part_name = "table2_" + table_prefix + str(index)

    # Create partition table for Table1
    cursor.execute(drop_query.format(table1_part_name))
    cursor.execute(create_query.format(table1_part_name, InputTable1))

    # Create partition table for Table2
    cursor.execute(drop_query.format(table2_part_name))
    cursor.execute(create_query.format(table2_part_name, InputTable2))

    # For the first partition table, the range is [0, lower]
    insert_query_first = " \
                INSERT INTO {0} \
                SELECT * FROM {1} \
                WHERE \
                {2} >= " + str(lower) + " AND {3} <= " + str(upper)

    # For rest of the partition tables, the range is (lower, upper]
    insert_query = " \
                INSERT INTO {0} \
                SELECT * FROM {1} \
                WHERE \
                {2} > " + str(lower) + " AND {3} <= " + str(upper)

    if index == 0:
        cursor.execute(insert_query_first.format(table1_part_name, InputTable1, Table1JoinColumn, Table1JoinColumn))
        cursor.execute(insert_query_first.format(table2_part_name, InputTable2, Table2JoinColumn, Table2JoinColumn))

    else:
        cursor.execute(insert_query.format(table1_part_name, InputTable1, Table1JoinColumn, Table1JoinColumn))
        cursor.execute(insert_query.format(table2_part_name, InputTable2, Table2JoinColumn, Table2JoinColumn))

    # Create the output partition table by joining partition tables from table 1 and table 2
    create_output_query = " \
                          CREATE TABLE " + output_part_prefix + str(index) + \
                          " AS SELECT * FROM " + table1_part_name + " INNER JOIN " + table2_part_name + \
                          " ON " + table1_part_name + "." + Table1JoinColumn + " = " + table2_part_name + "." + Table2JoinColumn
    cursor.execute(drop_query.format(output_part_prefix + str(index)))
    cursor.execute(create_output_query)


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cursor = openconnection.cursor()

    min_query = "SELECT MIN({}) FROM {}"
    max_query = "SELECT MAX({}) FROM {}"

    # Find minimum and maximum for Table 1
    cursor.execute(min_query.format(Table1JoinColumn, InputTable1))
    min1 = cursor.fetchone()[0]
    cursor.execute(max_query.format(Table1JoinColumn, InputTable1))
    max1 = cursor.fetchone()[0]

    # Find minimum and maximum for Table 2
    cursor.execute(min_query.format(Table2JoinColumn, InputTable2))
    min2 = cursor.fetchone()[0]
    cursor.execute(max_query.format(Table2JoinColumn, InputTable2))
    max2 = cursor.fetchone()[0]

    # Find the range
    min_val = min(min2, min1)
    max_val = max(max2, max1)
    range_val = float(max_val - min_val) / 5

    thread = [0] * 5
    table_prefix = "range_part"
    output_part_prefix = "output_part"

    # create partition tables for both tables and join
    for index in range(0, 5):
        lower = min_val + index * range_val
        upper = lower + max_val
        thread[index] = threading.Thread(
            target=join_partitions,
            args=(InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, table_prefix, output_part_prefix, index,
                  lower, upper, openconnection))
        thread[index].start()

    # Create the Output table
    create_query = "CREATE TABLE {} (LIKE {} INCLUDING ALL)"
    for index in range(0, 5):
        thread[index].join()
        if index == 0:
            cursor.execute(create_query.format(OutputTable, output_part_prefix + str(index)))

        insert_query = "INSERT INTO {} SELECT * FROM {}"
        cursor.execute(insert_query.format(OutputTable, output_part_prefix + str(index)))

    openconnection.commit()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
