from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

conn = None


def toRAM(data):
    return RAM(ramID=data['id'],
               company=data['company'],
               size=data['ramsize'])


def toQuery(data):
    return Query(queryID=data['id'],
                 purpose=data['purpose'],
                 size=data['size'])


def toDisk(data):
    return Disk(diskID=data['id'],
                company=data['manufacture'],
                speed=data['speed'],
                free_space=data['freesize'],
                cost=data['costbybyte']
                )


def dbConnection():
    global conn
    if conn is None:
        conn = Connector.DBConnector()
    return conn


def createTables():
    try:
        sql_query = sql.SQL("CREATE TABLE  Query ("
                            "id INTEGER PRIMARY KEY NOT NULL,"
                            "purpose TEXT NOT NULL,"
                            "size INTEGER NOT NULL,"
                            "CHECK (id > 0),"
                            "CHECK (size >= 0),"
                            "CHECK (purpose != 'None' )"
                            ");"
                            "CREATE TABLE Disk ("
                            "id INTEGER PRIMARY KEY NOT NULL,"
                            "manufacture TEXT NOT NULL,"
                            "speed INT NOT NULL,"
                            "freeSize INT NOT NULL,"
                            "totalSize INT NOT NULL,"
                            "costByByte INT NOT NULL,"
                            "CHECK (manufacture != 'None'),"
                            "CHECK (freeSize >= 0),"
                            "CHECK (freeSize <= totalSize),"
                            "CHECK (speed > 0),"
                            "CHECK (costByByte > 0),"
                            "CHECK (id > 0)"
                            ");"
                            "CREATE TABLE RAM("
                            "id INTEGER PRIMARY KEY,"
                            "ramSize INTEGER NOT NULL,"
                            "company TEXT NOT NULL,"
                            "CHECK (ramSize > 0),"
                            "CHECK (id > 0),"
                            "CHECK (company != 'None')"
                            ");"
                            "CREATE TABLE RunsON ("
                            "DiskId INTEGER,"
                            "QueryId INTEGER,"
                            "FOREIGN KEY (DiskId) REFERENCES Disk(id) ON DELETE CASCADE ,"
                            "FOREIGN KEY (QueryId) REFERENCES Query(id) ON DELETE CASCADE ,"
                            "UNIQUE (DiskId,QueryId)"
                            ");"
                            "CREATE TABLE AttachedTo ("
                            "RAMID INTEGER,"
                            "DiskId INTEGER,"
                            "FOREIGN KEY (RAMID) REFERENCES RAM(id) ON DELETE CASCADE ,"
                            "FOREIGN KEY (DiskId) REFERENCES Disk(id) ON DELETE CASCADE,"
                            "UNIQUE (RAMID,DiskId));"
                            )
        dbConnection().execute(sql_query)
    except Exception as e:
        print(e)
        dbConnection().rollback()
        return
    dbConnection().commit()


def clearTables():
    try:
        sql_query = sql.SQL(f"TRUNCATE table runson CASCADE; "
                            f"TRUNCATE table attachedto CASCADE; "
                            f"TRUNCATE table disk CASCADE; "
                            f"TRUNCATE table query CASCADE; "
                            f"TRUNCATE table ram CASCADE; ")
        dbConnection().execute(sql_query)
    except Exception as e:
        dbConnection().rollback()
        return
    dbConnection().commit()


def dropTables():
    try:
        query = sql.SQL(f"DROP TABLE disk CASCADE;"
                        f"DROP TABLE ram CASCADE;"
                        f"DROP TABLE query CASCADE;"
                        f"DROP TABLE runson CASCADE;"
                        f"DROP TABLE attachedto CASCADE;")
        dbConnection().execute(query)
    except Exception as e:
        dbConnection().rollback()
        return
    dbConnection().commit()


def addQuery(query: Query) -> ReturnValue:
    if query is None or query == Query.badQuery():
        return ReturnValue.BAD_PARAMS
    result = ReturnValue.OK
    try:
        sql_query = sql.SQL(
            f"INSERT INTO query (id, purpose, size) VALUES ({query.getQueryID()},'{query.getPurpose()}',{query.getSize()})")
        dbConnection().execute(sql_query)
    except DatabaseException.CHECK_VIOLATION:
        result = ReturnValue.BAD_PARAMS
        dbConnection().rollback()
    except DatabaseException.UNIQUE_VIOLATION:
        result = ReturnValue.ALREADY_EXISTS
        dbConnection().rollback()
    except Exception as e:

        result = ReturnValue.ERROR
        dbConnection().rollback()
    dbConnection().commit()
    return result


def getQueryProfile(queryID: int) -> Query:
    try:
        sql_query = sql.SQL(f"SELECT * FROM query WHERE id = {queryID}")
        rows, result = dbConnection().execute(sql_query)
        if rows == 0:
            return Query.badQuery()
        result = result[0]
        query = toQuery(result)
        return query
    except Exception as e:
        dbConnection().rollback()
    return Query.badQuery()


def deleteQuery(query: Query) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"UPDATE disk SET freeSize = freeSize + (SELECT size FROM query WHERE id = {query.getQueryID()})"
            f"WHERE id IN ((SELECT diskid FROM runson WHERE queryid = {query.getQueryID()}));"
            f"DELETE FROM query WHERE id = {query.getQueryID()}")

        dbConnection().execute(sql_query)
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    if disk is None or disk == Disk.badDisk():
        return ReturnValue.BAD_PARAMS
    try:
        sql_query = sql.SQL(
            f"INSERT INTO disk (id, manufacture, speed, freesize,totalSize, costbybyte) VALUES "
            f"({disk.getDiskID()},"
            f"'{disk.getCompany()}',"
            f"{disk.getSpeed()},"
            f"{disk.getFreeSpace()},"
            f"{disk.getFreeSpace()},"
            f"{disk.getCost()}"
            f")")
        dbConnection().execute(sql_query)
    except DatabaseException.CHECK_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.database_ini_ERROR as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    try:
        sql_query = sql.SQL(f"SELECT * FROM disk WHERE id = {diskID}")
        rows, result = dbConnection().execute(sql_query)
        if rows != 1:
            return Disk.badDisk()
        result = result[0]
        dbConnection().commit()
        return toDisk(result)
    except Exception as e:
        dbConnection().rollback()
    return Disk.badDisk()


def deleteDisk(diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM disk WHERE id = {diskID}")
        rows, result = dbConnection().execute(sql_query)
        if rows == 0:
            dbConnection().rollback()
            return ReturnValue.NOT_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


# INSERT INTO ram VALUE ()
def addRAM(ram: RAM) -> ReturnValue:
    if ram is None or ram == ram.badRAM():
        return ReturnValue.BAD_PARAMS
    try:
        sql_query = sql.SQL(
            f"INSERT INTO ram (id, ramsize, company)  VALUES "
            f"({ram.getRamID()},"
            f"{ram.getSize()},"
            f"'{ram.getCompany()}'"
            f")")
        dbConnection().execute(sql_query)
    except DatabaseException.CHECK_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    try:
        sql_query = sql.SQL(f"SELECT * FROM ram WHERE id = {ramID}")
        rows, result = dbConnection().execute(sql_query)
        if rows != 1:
            dbConnection().rollback()
            return RAM.badRAM()
        result = result[0]
        return toRAM(result)
    except Exception as e:
        dbConnection().rollback()
    return RAM.badRAM()


def deleteRAM(ramID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM ram WHERE id = {ramID} ")
        rows, result = dbConnection().execute(sql_query)
        if rows == 0:
            dbConnection().rollback()
            return ReturnValue.NOT_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"INSERT INTO disk (id, manufacture, speed, freesize,totalsize, costbybyte) VALUES "
                            f"({disk.getDiskID()},"
                            f"'{disk.getCompany()}',"
                            f"{disk.getSpeed()},"
                            f"{disk.getFreeSpace()},"
                            f"{disk.getFreeSpace()},"
                            f"{disk.getCost()}"
                            f");"
                            f"INSERT INTO query (id, purpose, size) VALUES ("
                            f"{query.getQueryID()},"
                            f"'{query.getPurpose()}',"
                            f"{query.getSize()})")
        dbConnection().execute(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        # print(e)
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"INSERT INTO runson (diskid,queryid) VALUES ({diskID}, {query.getQueryID()}); "
            f"UPDATE disk SET freeSize = freeSize - (SELECT size FROM query WHERE id = {query.getQueryID()})"
            f"WHERE id = {diskID};")

        dbConnection().execute(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        dbConnection().rollback()
        # print(e)
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"UPDATE disk SET freesize = freesize + COALESCE("
            f"(SELECT size FROM query q INNER JOIN runson r ON q.id = r.queryid WHERE q.id = {query.getQueryID()} AND diskid = {diskID})"
            f",0)"
            f"WHERE disk.id = {diskID};"
            f"DELETE FROM runson WHERE diskid = {diskID} AND queryid = {query.getQueryID()};"
        )
        dbConnection().execute(sql_query)
    except Exception as e:
        dbConnection().rollback()
        # print(e)
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"INSERT INTO attachedto (ramid,diskid) VALUES ({ramID},{diskID}); "
        )
        rows, result = dbConnection().execute(sql_query)
    except DatabaseException.UNIQUE_VIOLATION as e:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        # print(e)
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM attachedto WHERE ramid={ramID} AND diskid={diskID}")
        rows, result = dbConnection().execute(sql_query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
    dbConnection().commit()
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    result = None
    try:
        sql_query = sql.SQL(f"SELECT AVG(size) "
                            f"FROM runson INNER JOIN query ON runson.queryid = query.id "
                            f"WHERE diskid = {diskID} ;")
        _, result = dbConnection().execute(sql_query)
        if result.rows[0][0] is None:
            return 0
    except Exception as e:
        # print(e)
        return -1
    return result.rows[0][0]


def diskTotalRAM(diskID: int) -> int:
    result = None
    try:
        sql_query = sql.SQL(f"SELECT COALESCE(SUM(ramsize),0) "
                            f"FROM attachedto INNER JOIN ram ON ram.id = attachedto.ramid "
                            f"WHERE attachedto.diskid = {diskID} ")
        _, result = dbConnection().execute(sql_query)
    except Exception as e:
        # print(e)
        return -1
    return result.rows[0][0]


def getCostForPurpose(purpose: str) -> int:
    result = None
    try:
        sql_query = sql.SQL(f"SELECT COALESCE(SUM(outer_cost*inner_size),0) "
                            f"FROM ("
                            f"SELECT inner_size, disk.costbybyte AS outer_cost "
                            f"FROM "
                            f"("
                            f"SELECT runson.diskid AS inner_diskid, query.size AS inner_size "
                            f"FROM query INNER JOIN runson ON query.id = runson.queryid "
                            f"WHERE query.purpose = '{purpose}'"
                            f") AS query_purrpose_and_runs"
                            f"INNER JOIN disk ON disk.id = inner_diskid "
                            f") AS outer_calc; "
                            )
        _, result = dbConnection().execute(sql_query)
    except Exception as e:
        # print(e)
        return -1

    return result.rows[0][0]


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    try:
        sql_query = sql.SQL(f"SELECT id "
                            f"FROM query "
                            f"WHERE size <= (SELECT freesize FROM disk WHERE id = {diskID}) "
                            f"ORDER BY id DESC "
                            f"LIMIT 5")
        _, result = dbConnection().execute(sql_query)
    except Exception as e:
        # print(e)
        return []
    return [i[0] for i in result.rows]


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    try:
        sql_query = sql.SQL(f"SELECT id FROM query WHERE "
                            f"query.size <= (SELECT freesize FROM disk WHERE id = {diskID}) "
                            f"AND "
                            f"query.size <= (SELECT COALESCE(SUM(ramsize),0) FROM ram WHERE id IN (SELECT ramid FROM attachedto WHERE diskid = {diskID})) "
                            f"ORDER BY id ASC "
                            f"LIMIT 5")
        _, result = dbConnection().execute(sql_query)
    except Exception as e:
        dbConnection().rollback()
        # print(e)
        return []
    dbConnection().commit()
    return [i[0] for i in result.rows]


def isCompanyExclusive(diskID: int) -> bool:
    try:
        sql_query = sql.SQL(f"SELECT COUNT(id) "
                            f"FROM disk "
                            f"WHERE id = {diskID} AND manufacture = ALL ("
                            f"  SELECT company"
                            f"  FROM ram"
                            f"  WHERE id IN ("
                            f"    SELECT ramid"
                            f"    FROM attachedto"
                            f"    WHERE diskid = {diskID}"
                            f"   )"
                            f")")
        rows, result = dbConnection().execute(sql_query)
    except Exception as e:
        # print(e)
        dbConnection().rollback()
        return False
    return result.rows[0][0] == 1


def getConflictingDisks() -> List[int]:
    try:
        # sql_query = sql.SQL(f"SELECT diskid AS ddid "
        #                     f"FROM runson "
        #                     f"WHERE queryid IN ("
        #                     f"      SELECT queryid FROM runson WHERE diskid <> ddid);"
        #                     f"")
        sql_query = sql.SQL("SELECT DISTINCT diskid "
                            "FROM runson "
                            "WHERE runson.queryid IN ("
                            "   SELECT queryid"
                            "   FROM ("
                            "       SELECT COUNT(queryid) as n_of_disks, queryid"
                            "       FROM runson"
                            "       GROUP BY queryid"
                            "       ORDER BY n_of_disks DESC, queryid"
                            "       ) AS t"
                            "   WHERE n_of_disks > 1)  "
                            "ORDER BY diskid")
        rows, results = dbConnection().execute(sql_query)
        return [results[i]['diskid'] for i in range(results.size())]
    except Exception as e:
        dbConnection().rollback()
        return []
        return ReturnValue.ERROR


def mostAvailableDisks() -> List[int]:
    try:
        sql_query = sql.SQL(
            f"SELECT sum(case cast(disk.id as boolean) when CAST(query.id as boolean) IS NOT NULL then 1 else 0 end) as n_queries, disk.id "
            f"FROM disk "
            f"         LEFT JOIN query "
            f"                   ON query.size <= disk.freesize "
            f"GROUP BY disk.id, disk.speed "
            f"ORDER BY n_queries DESC, disk.speed DESC, disk.id ASC "
            f"LIMIT 5 "

        )
        rows, results = dbConnection().execute(sql_query)
        return [results[i]['id'] for i in range(results.size())]
    except Exception as e:
        dbConnection().rollback()
        return []


def getCloseQueries(queryID: int) -> List[int]:
    try:

        sql_query = sql.SQL(f"SELECT queryid "
                            f"FROM ( "
                            f"         SELECT COALESCE(count(queryid),0) as n_of_disks, queryid "
                            f"         FROM runson "
                            f"         WHERE diskid in ( "
                            f"             SELECT diskid "
                            f"             FROM runson "
                            f"             WHERE queryid = {queryID} "
                            f"         ) "
                            f"           AND queryid != {queryID} "
                            f"         GROUP BY queryid "
                            f"     ) AS t "
                            f"WHERE t.n_of_disks >= COALESCE((SELECT count(diskid) FROM runson where queryid = {queryID}),0)*0.5 "
                            f"ORDER BY queryid ASC "
                            f"LIMIT 10"
                            )
        rows, results = dbConnection().execute(sql_query)
        return [results[i]['queryid'] for i in range(results.size())]
    except Exception:
        pass
    return []


# dropTables()
# createTables()

# query1 = Query(queryID=1, purpose='teze', size=10)
# query2 = Query(queryID=2, purpose='stam', size=20)
# query6 = Query(queryID=6, purpose='stam', size=20)
# addQuery(query1)
# deleteQuery(query)
# print(getQueryProfile(2))
# deleteQuery(query)
# disk1 = Disk(1, 'apple', 1, 2, 3)
# disk2 = Disk(2, 'samsung', 13, 50, 2)
# print(addQueryToDisk(query, disk1.getDiskID()))
# print(addQueryToDisk(query, disk2.getDiskID()))
# addDisk(disk)
# print(getDiskProfile(disk.getDiskID()))
# ram = RAM(10, 'aaaaaa', 100)
# addRAM(ram)
# print(addDiskAndQuery(disk, query))
# print(removeQueryFromDisk(query, 1))
# dropTable('attachedto')
# createAttachedToTable()
# print(addRAMToDisk(1,1))
# print(addRAMToDisk(1,2))
# print(addRAMToDisk(1,2))
# print(addRAMToDisk(999,3))
# print(addRAMToDisk(3,999))
# addRAMToDisk(1,1)
# addRAMToDisk(2,2)
# addRAMToDisk(3,3)
# addRAMToDisk(4,4)
#
# print(removeRAMFromDisk(999,1))
# print(removeRAMFromDisk(1,999))
# print(removeRAMFromDisk(999,999))
# print(removeRAMFromDisk(1,2))
# print(removeRAMFromDisk(999,1))
# print(removeRAMFromDisk(1,999))
# print(removeRAMFromDisk(1,1))

# print(addRAMToDisk(1, 2))
# print(addRAMToDisk(3, 2))
#
# print(diskTotalRAM(2))

# print(getCostForPurpose('c'))
# print(addQueryToDisk(query1,1))
# print(addQueryToDisk(query6,1))
# print(addQueryToDisk(query2,1))

# print(addRAMToDisk(1,1))
# print(addRAMToDisk(2,1))
# print(getQueriesCanBeAddedToDiskAndRAM(1))
# print(addRAMToDisk(4,1))
# print(getQueriesCanBeAddedToDiskAndRAM(1))

# CLEAR
dropTables()
# createTables()

# ADDING DISKS
# disk1 = Disk(1, 'APPLE', 15, 50, 1)
# disk2 = Disk(2, 'APPLE', 15, 30, 1)
# uniquedisk = Disk(3, 'SAMSUNG', 15, 6, 7)
# assert (ReturnValue.OK == addDisk(disk1))
# assert (ReturnValue.OK == addDisk(disk2))
# assert (ReturnValue.OK == addDisk(uniquedisk))

# ADDING QUERIES
# query1 = Query(queryID=1, purpose='stam', size=10)
# query2 = Query(queryID=2, purpose='stam', size=15)
# query3 = Query(queryID=3, purpose='stam', size=25)
# query4 = Query(queryID=4, purpose='stam', size=20)
# query5 = Query(queryID=5, purpose='stam', size=40)
# print(addQuery(query1))
# print(addQuery(query2))
# print(addQuery(query3))
# print(addQuery(query4))
# print(addQuery(query5))

# ADDING RAMS
# ram1 = RAM(1, 'APPLE', 8)
# assert (ReturnValue.OK == addRAM(ram1))
# print("should be true")
# print(isCompanyExclusive(1))
# print("should be true")
# print(isCompanyExclusive(2))
# print("should be false")
# print(isCompanyExclusive(3))
#
# ram2 = RAM(2, 'SAMSUNG', 16)
# assert (ReturnValue.OK == addRAM(ram2))
# print("should be false")
# print(isCompanyExclusive(1))
#
#
#
# ram3 = RAM(3, 'NIVIDIA', 16)
# assert (ReturnValue.OK == addRAM(ram3))

#
# print("should be empty :")
# print(getQueriesCanBeAddedToDiskAndRAM(1))
#
# print(addRAMToDisk(3,1))
# print("should return query 1, 2")
# print(getQueriesCanBeAddedToDiskAndRAM(1))
#
# print(addRAMToDisk(2,1))
# print("should return query 1, 2, 3, 4")
# print(getQueriesCanBeAddedToDiskAndRAM(1))
#
# print("should be empty :")
# print(getQueriesCanBeAddedToDiskAndRAM(2))
#
# print(addRAMToDisk(2,2))
# print("should return query 1, 2")
# print(getQueriesCanBeAddedToDiskAndRAM(1))
#
# print(addRAMToDisk(2,2))
# print("should return query 1, 2")
# print(getQueriesCanBeAddedToDiskAndRAM(2))


# print(isCompanyExclusive(1))
# print(isCompanyExclusive(3))
# print(isCompanyExclusive(1))
# print(isCompanyExclusive(2))
# print(isCompanyExclusive(1))
