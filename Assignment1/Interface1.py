import psycopg2
import os
import sys

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()

    # Drop the Rating table if exists
    cursor.execute("DROP TABLE IF EXISTS " + ratingstablename)

    # Create the Rating table
    # col2, col4 and col6 are columns to separate the data according to the ":" separator instead of "::"
    create_query = " \
            CREATE TABLE " + ratingstablename + " ( \
            userid INT, \
            col2 VARCHAR(5),\
            movieid INT, \
            col4 VARCHAR(5),\
            rating REAL, \
            col6 VARCHAR(5),\
            Timestamp INT \
    )"
    cursor.execute(create_query)

    openconnection.commit()

    # load the ratings data into the table
    with open(ratingsfilepath, 'r') as file:
        cursor.copy_from(file, ratingstablename, sep=':')

    # Drop the temporary columns and the timestamp column
    cursor.execute(
        "ALTER TABLE " + ratingstablename + " \
            DROP COLUMN col2, \
            DROP COLUMN col4, \
            DROP COLUMN col6, \
            DROP COLUMN Timestamp \
    ")

    openconnection.commit()
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions == 0:
        return

    cursor = openconnection.cursor()

    # Query to drop a table
    drop_query = "DROP TABLE IF EXISTS {}"

    # Table to store the meta data of the partitions
    create_range_partition_query = " \
                CREATE TABLE range_partitions ( \
                pindex VARCHAR(20), \
                lower REAL, \
                upper REAL \
    )"
    cursor.execute(drop_query.format('range_partitions'))
    cursor.execute(create_range_partition_query)

    # For the first partition table, the range is [0, parition_size]
    create_query_first = " \
                CREATE TABLE {} \
                AS SELECT * FROM " + ratingstablename + " \
                WHERE Rating >= {} AND Rating <=  {}"

    # For rest of the partition tables, the range is (lower, upper]
    create_query = " \
                CREATE TABLE {} \
                AS SELECT * FROM " + ratingstablename + "\
                WHERE Rating > {} AND Rating <=  {}"

    parition_size = round(5 / numberofpartitions, 2)
    upper = 0
    for index in range(numberofpartitions):
        lower = upper
        upper = lower + parition_size

        # Insert the meta data values (partition index, lower range and upper range) to the 'range_partitions' table
        insert_query = " \
                INSERT INTO range_partitions \
                (pindex, lower, upper) VALUES \
                ({}, {}, {})"
        cursor.execute(insert_query.format(index, lower, upper))

        if index == 0:
            cursor.execute(drop_query.format(RANGE_TABLE_PREFIX + str(index)))
            cursor.execute(
                create_query_first.format(
                    RANGE_TABLE_PREFIX + str(index),
                    lower,
                    upper
                )
            )
        else:
            cursor.execute(drop_query.format(RANGE_TABLE_PREFIX + str(index)))
            cursor.execute(
                create_query.format(
                    RANGE_TABLE_PREFIX + str(index),
                    lower,
                    upper
                )
            )

    openconnection.commit()
    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions == 0:
        return

    cursor = openconnection.cursor()

    # Query to drop a table
    drop_query = "DROP TABLE IF EXISTS {}"

    # Create query
    create_query = " \
            CREATE TABLE {} \
            AS SELECT userid, movieid, rating FROM  \
            ( SELECT *, ROW_NUMBER() OVER() as row_num FROM " + ratingstablename + " ) as helper_table \
            WHERE (row_num - 1) % {} = {}"

    # Keep track of the table index for the round robin insert functionality
    cursor.execute("SELECT COUNT (*) FROM " + ratingstablename)
    table_size = cursor.fetchone()[0]
    table_index = 0

    for index in range(numberofpartitions):
        cursor.execute(drop_query.format(RROBIN_TABLE_PREFIX + str(index)))
        cursor.execute(
            create_query.format(
                RROBIN_TABLE_PREFIX + str(index),
                numberofpartitions,
                index
            )
        )

        cursor.execute("SELECT COUNT (*) FROM " + RROBIN_TABLE_PREFIX + str(index))
        number_of_rows = cursor.fetchone()[0]

        # Update the table_index if the no of rows are lesser, as this is the table to be used for next rrobin insertion
        if number_of_rows < table_size:
            table_size = number_of_rows
            table_index = index

    cursor.execute(drop_query.format('rrobin_partition'))
    cursor.execute(" \
            CREATE TABLE rrobin_partition ( \
            partition_index INT, \
            numberofpartitions INT \
    )")
    insert_query = " \
            INSERT INTO rrobin_partition \
            (partition_index, numberofpartitions) VALUES \
            ({}, {}) "
    cursor.execute(insert_query.format(str(table_index), str(numberofpartitions)))

    openconnection.commit()
    cursor.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    # Get the correct fragment index and the numberofpartition from the rrobin_partition table
    cursor.execute("SELECT * FROM rrobin_partition")
    rrobin_meta_data = cursor.fetchone()
    pindex = rrobin_meta_data[0]
    numberofpartition = rrobin_meta_data[1]
    partition_table = pindex % numberofpartition

    # Insert query
    insert_query = "INSERT INTO {} (userid, movieid, rating) VALUES ({}, {}, {})"

    # Insert the data into the ratings table
    cursor.execute(insert_query.format(ratingstablename, userid, itemid, rating))

    # Insert the data into the correct fragment
    cursor.execute(insert_query.format(RROBIN_TABLE_PREFIX + str(partition_table), userid, itemid, rating))

    # Update the rrobin_partition table with the next partition table
    cursor.execute("UPDATE rrobin_partition SET partition_index = " + str(partition_table + 1))

    openconnection.commit()
    cursor.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    pindex = 0

    # Find the correct partition table index
    find_partition_query = " \
            SELECT pindex FROM range_partitions \
            WHERE " + str(rating) + " > lower AND " + str(rating) + " <= upper \
    "
    cursor.execute(find_partition_query)
    row = cursor.fetchone()
    if (row != None):
        pindex = row[0]

    # Insert query
    insert_query = " INSERT INTO {} (userid, movieid, rating) VALUES ({}, {}, {})"

    # Insert the data into the ratings table
    cursor.execute(insert_query.format(ratingstablename, userid, itemid, rating))

    # Insert the data into the correct fragment
    cursor.execute(insert_query.format(RANGE_TABLE_PREFIX + str(pindex), userid, itemid, rating))

    openconnection.commit()
    cursor.close()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    cursor = openconnection.cursor()

    outputQueryList = []
    select_query = "SELECT '{}' AS tablename, userid, movieid, rating FROM {} WHERE rating >= {} AND rating <= {}"

    # Range Partitions
    cursor.execute("SELECT COUNT(*) FROM range_partitions")
    range_partitions_count = cursor.fetchone()[0]

    for index in range(range_partitions_count):
        outputQueryList.append(
            select_query.format(
                RANGE_TABLE_PREFIX + str(index),
                RANGE_TABLE_PREFIX + str(index),
                ratingMinValue,
                ratingMaxValue
            )
        )

    # Round Robin Partitions
    cursor.execute("SELECT numberofpartitions FROM rrobin_partition")
    rrobin_partitions_count = cursor.fetchone()[0]

    for index in range(rrobin_partitions_count):
        outputQueryList.append(
            select_query.format(
                RROBIN_TABLE_PREFIX + str(index),
                RROBIN_TABLE_PREFIX + str(index),
                ratingMinValue,
                ratingMaxValue
            )
        )

    union_query = "SELECT * FROM ({}) AS output".format(' UNION ALL '.join(outputQueryList))

    # Copy the union of all queries to the file
    with open(outputPath, 'w') as file:
        cursor.execute(" \
            COPY (" + union_query + " ) \
            TO '" + os.path.abspath(file.name) + "' \
            (FORMAT text, DELIMITER ',')")

    openconnection.commit()
    cursor.close()


def pointQuery(ratingValue, openconnection, outputPath):
    cursor = openconnection.cursor()

    outputQueryList = []
    select_query = "SELECT '{}' AS tablename, userid, movieid, rating FROM {} WHERE rating = {}"

    # Range Partitions
    cursor.execute("SELECT COUNT(*) FROM range_partitions")
    range_partitions_count = cursor.fetchone()[0]

    for index in range(range_partitions_count):
        outputQueryList.append(
            select_query.format(
                RANGE_TABLE_PREFIX + str(index),
                RANGE_TABLE_PREFIX + str(index),
                ratingValue
            )
        )

    # Round Robin Partitions
    cursor.execute("SELECT numberofpartitions FROM rrobin_partition")
    rrobin_partitions_count = cursor.fetchone()[0]

    for i in range(rrobin_partitions_count):
        outputQueryList.append(
            select_query.format(
                RROBIN_TABLE_PREFIX + str(i),
                RROBIN_TABLE_PREFIX + str(i),
                ratingValue
            )
        )

    union_query = "SELECT * FROM ({}) AS output".format(' UNION ALL '.join(outputQueryList))

    # Copy the union of all queries to the file
    with open(outputPath, 'w') as file:
        cursor.execute(" \
            COPY (" + union_query + ") \
            TO '" + os.path.abspath(file.name) + "' \
            (FORMAT text, DELIMITER ',')")

    openconnection.commit()
    cursor.close()


def createDB(dbname='dds_assignment1'):
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
    con.close()


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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
