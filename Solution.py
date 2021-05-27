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


def executeAndCommit(sql_query):
    rows, result = dbConnection().execute(sql_query)
    dbConnection().commit()
    return rows, result


def createQueryTable():
    try:
        query = sql.SQL("CREATE TABLE  Query ("
                        "id INTEGER PRIMARY KEY NOT NULL,"
                        "purpose TEXT NOT NULL,"
                        "size INTEGER NOT NULL"
                        ") ")
        executeAndCommit(query)
    except Exception as e:
        pass


def createDiskTable():
    try:
        query = sql.SQL("CREATE TABLE Disk ("
                        "id INTEGER PRIMARY KEY NOT NULL,"
                        "manufacture TEXT NOT NULL,"
                        "speed INT NOT NULL,"
                        "freeSize INT NOT NULL,"
                        "totalSize INT NOT NULL,"
                        "costByByte INT NOT NULL,"
                        "CHECK (freeSize >= 0),"
                        "CHECK (freeSize <= totalSize)"
                        ")")
        executeAndCommit(query)
    except Exception as e:
        pass


def createRAMTable():
    try:

        query = sql.SQL("CREATE TABLE RAM("
                        "id INTEGER PRIMARY KEY,"
                        "ramSize INTEGER NOT NULL,"
                        "company TEXT NOT NULL,"
                        "DiskId INT REFERENCES disk(id) ON DELETE CASCADE "
                        ")")
        executeAndCommit(query)
    except Exception as e:
        print(e)


def createRunsOnTable():
    try:

        query = sql.SQL("CREATE TABLE RunsON ("
                        "DiskId INTEGER,"
                        "QueryId INTEGER,"
                        "FOREIGN KEY (DiskId) REFERENCES Disk(id) ON DELETE CASCADE ,"
                        "FOREIGN KEY (QueryId) REFERENCES Query(id) ON DELETE CASCADE ,"
                        "UNIQUE (DiskId,QueryId)"
                        ")")
        executeAndCommit(query)
    except Exception as e:
        pass


def createTables():
    createQueryTable()
    createDiskTable()
    createRAMTable()
    createRunsOnTable()


def clearTables():
    pass


def dropTable(table_name):
    query = sql.SQL(f"DROP TABLE {table_name} CASCADE")
    executeAndCommit(query)


def dropTables():
    dropTable("disk")
    dropTable("ram")
    dropTable('query')
    dropTable('runson')


def addQuery(query: Query) -> ReturnValue:
    if query is None or query == Query.badQuery():
        return ReturnValue.BAD_PARAMS
    result = ReturnValue.OK
    try:
        sql_query = sql.SQL(
            f"INSERT INTO query (id, purpose, size) VALUES ({query.getQueryID()},'{query.getPurpose()}',{query.getSize()})")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        result = ReturnValue.ALREADY_EXISTS
    except Exception:
        result = ReturnValue.ERROR
    dbConnection().rollback()
    return result


def getQueryProfile(queryID: int) -> Query:
    try:
        sql_query = sql.SQL(f"SELECT * FROM query WHERE id = {queryID}")
        rows, result = executeAndCommit(sql_query)
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

        executeAndCommit(sql_query)
    except Exception as e:
        dbConnection().rollback()
        return ReturnValue.ERROR
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
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR

    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    try:
        sql_query = sql.SQL(f"SELECT * FROM disk WHERE id = {diskID}")
        rows, result = executeAndCommit(sql_query)
        if rows != 1:
            return Disk.badDisk()
        result = result[0]
        return toDisk(result)
    except Exception as e:
        dbConnection().rollback()
    return Disk.badDisk()


def deleteDisk(diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM disk WHERE id = {diskID}")
        rows, result = executeAndCommit(sql_query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR
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
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR

    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    try:
        sql_query = sql.SQL(f"SELECT * FROM ram WHERE id = {ramID}")
        rows, result = executeAndCommit(sql_query)
        if rows != 1:
            return RAM.badRAM()
        result = result[0]
        return toRAM(result)
    except Exception as e:
        dbConnection().rollback()
    return RAM.badRAM()


def deleteRAM(ramID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"DELETE FROM disk WHERE id = {ramID}")
        rows, result = executeAndCommit(sql_query)
        if rows == 0:
            return ReturnValue.NOT_EXISTS
    except Exception:
        dbConnection().rollback()
        return ReturnValue.ERROR
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    try:
        sql_query = sql.SQL(f"INSERT INTO disk (id, manufacture, speed, freesize, costbybyte) VALUES "
                            f"({disk.getDiskID()},"
                            f"'{disk.getCompany()}',"
                            f"{disk.getSpeed()},"
                            f"{disk.getFreeSpace()},"
                            f"{disk.getCost()}"
                            f");"
                            f"INSERT INTO query (id, purpose, size) VALUES ("
                            f"{query.getQueryID()},"
                            f"'{query.getPurpose()}',"
                            f"{query.getSize()})")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        dbConnection().rollback()
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
        dbConnection().rollback()
        return ReturnValue.ERROR
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    try:
        sql_query = sql.SQL(
            f"UPDATE disk SET freeSize = freeSize - (SELECT size FROM query WHERE id = {query.getQueryID()})"
            f"WHERE id = {diskID};"
            f"INSERT INTO runson (diskid,queryid) VALUES ({diskID}, {query.getQueryID()})")
        executeAndCommit(sql_query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except Exception:
        return ReturnValue.ERROR
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []


# dropTables()
# createTables()
query = Query(queryID=1, purpose='stam', size=10)
# addQuery(query)
# deleteQuery(query)
# print(getQueryProfile(2))
# deleteQuery(query)
disk = Disk(1, 'apple', 1, 2, 3)
print(addQueryToDisk(query, disk.getDiskID()))
# addDisk(disk)
# print(getDiskProfile(disk.getDiskID()))
ram = RAM(10, 'aaaaaa', 100)
# addRAM(ram)
# print(addDiskAndQuery(disk, query))
