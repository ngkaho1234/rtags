#include "TableDatabase.h"
#include "Project.h"
#include <sqlite3.h>

class TableDatabaseTxn
{
public:
    //
    // Begins the SQLite transaction Exception is thrown in case of error, then the Transaction is NOT initiated.
    //
    TableDatabaseTxn(SQLite::Database &database)
        : mDatabase(database), mbCommited(false)
    {
        mDatabase.exec("BEGIN IMMEDIATE");
    }

    //
    // Safely rollback the transaction if it has not been committed.
    //
    ~TableDatabaseTxn() noexcept // nothrow
    {
        if (!mbCommited) {
            try {
                mDatabase.exec("ROLLBACK");
            } catch (SQLite::Exception &) {
                // Never throw an exception in a destructor: error if already rollbacked, but no harm is caused by this.
            }
        }
    }

    //
    // Transaction commit method
    //
    void commit()
    {
        if (!mbCommited) {
            do {
                try {
                    mDatabase.exec("COMMIT");
                    mbCommited = true;
                } catch (SQLite::Exception &e) {
                    if (e.getErrorCode() != SQLITE_BUSY)
                        throw;
                    continue;
                }
                break;
            } while (1);
        } else {
            throw SQLite::Exception("Transaction already commited.");
        }
    }

private:
    // Transaction must be non-copyable
    TableDatabaseTxn(const TableDatabaseTxn &);
    TableDatabaseTxn &operator=(const TableDatabaseTxn &);

    // Reference to SQLite Database connection
    SQLite::Database &mDatabase;
    // Set this to true when commit succeeds
    bool mbCommited;
};

static constexpr auto DatabaseNames = { Project::SymbolNames, Project::Targets, Project::Usrs };

//
// The schema of all tables in the database is in this form:
// CREATE TABLES @tableName (FileId integer, Key BLOB)
//
static String PrepareTableStatement(const String &tableName)
{
    String ret = "CREATE TABLE IF NOT EXISTS ";
    ret += tableName;
    ret += " (FileId integer, Key BLOB); ";
    return ret;
}

//
// The schema of all indice in the database is in this form:
// CREATE INDEX IxFileId_@tableName (FileId)
//
static String PrepareIndexStatement(const String &tableName)
{
    String ret = "CREATE INDEX IF NOT EXISTS IxFileId_";
    ret += tableName;
    ret += " ON ";
    ret += tableName;
    ret += " (FileId); ";
    ret += "CREATE INDEX IF NOT EXISTS IxKey_";
    ret += tableName;
    ret += " ON ";
    ret += tableName;
    ret += " (Key); ";
    return ret;
}

#define DATABASE_CHECKPOINT_FRAMES 4096
#define DATABASE_HARD_CHECKPOINT_FRAMES (DATABASE_CHECKPOINT_FRAMES * 3 / 2)

int TableDatabase::TableDatabaseCommitCallback(void *arg, sqlite3 *, const char *databaseName, int numberFrames)
{
    int ret = SQLITE_OK;
    TableDatabase *tableDatabase = static_cast<TableDatabase *>(arg);
    if (numberFrames >= DATABASE_CHECKPOINT_FRAMES) {
on_retry:
        try {
            if (numberFrames >= DATABASE_HARD_CHECKPOINT_FRAMES) {
                tableDatabase->mDatabase.walCheckpoint(databaseName, SQLITE_CHECKPOINT_FULL, NULL, NULL);
            } else {
                tableDatabase->mDatabase.walCheckpoint(databaseName, SQLITE_CHECKPOINT_PASSIVE, NULL, NULL);
            }
        } catch (SQLite::Exception &e) {
            if (e.getErrorCode() == SQLITE_BUSY)
                goto on_retry;
            ret = e.getErrorCode();
        }
    }
    return ret;
}

//
// Table database open method
//
TableDatabase::TableDatabase(const Path &path, bool create) try
    : mCreated(create),
      mDatabase(path, create ? SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE : SQLite::OPEN_READWRITE, 1000)
{
    mDatabase.exec("pragma journal_mode = WAL;");
    mDatabase.exec("pragma synchronous = off;");
    mDatabase.exec("pragma journal_size_limit = 33554432;");
    mDatabase.walCommitHook(TableDatabaseCommitCallback, this);
    if (create) {
        TableDatabaseTxn txn(this->mDatabase);
        for (auto i : DatabaseNames) {
            String stmt = PrepareTableStatement(Project::fileMapName(static_cast<Project::FileMapType>(i)));
            mDatabase.exec(stmt);
            stmt = PrepareIndexStatement(Project::fileMapName(static_cast<Project::FileMapType>(i)));
            mDatabase.exec(stmt);
        }
        txn.commit();
    }
} catch (SQLite::Exception &e) {
    throw TableDatabaseException(e);
}

TableDatabase::~TableDatabase()
{
}

//
// Delete all entries from table database whose FileId is the caller-specified FileId
//
static void TableDatabaseDeleteUnit(SQLite::Database &database, uint32_t fileId)
{
    for (auto i : DatabaseNames) {
        String stmt = String("DELETE FROM ") + Project::fileMapName(static_cast<Project::FileMapType>(i)) +
                      " WHERE FileId = ?";
        SQLite::Statement query(database, stmt);
        query.bind(1, fileId);
        query.exec();
    }
}

//
// Insert @map into the specified table
// Key has to be indexable type. Must be called within a transaction
//
template <typename MapType>
static void TableDatabaseInsert(SQLite::Database &database,
                                Project::FileMapType fileMap, uint32_t fileId,
                                const MapType &map)
{
    String stmt = String("INSERT INTO ") + Project::fileMapName(static_cast<Project::FileMapType>(fileMap)) +
                  " (FileId, Key) Values (?, ?)";
    SQLite::Statement query(database, stmt);
    for (auto pair : map) {
        query.bind(1, fileId);
        query.bind(2, pair.first.data(), pair.first.size());

        query.exec();
        query.reset();
    }
}

//
// Query database
//
template <typename KeyType>
static void TableDatabaseQuery(SQLite::Database &database,
                               Project::FileMapType fileMap, uint32_t fileId,
                               const KeyType &key, bool isKeyPrefix,
                               std::function<TableDatabase::QueryResult(uint32_t fileId, const KeyType &key)> cb)
{
    String stmt = String("SELECT * FROM ") + Project::fileMapName(static_cast<Project::FileMapType>(fileMap)) +
                  " WHERE " +
                  (fileId ? "FileID = ? AND " : "") +
                  (isKeyPrefix ? "Key >= ?" : "Key = ?") +
                  " ORDER BY Key ASC";
    SQLite::Statement query(database, stmt);

    if (!fileId) {
        query.bind(1, key.data(), key.size());
    } else {
        query.bind(1, fileId);
        query.bind(2, key.data(), key.size());
    }

    while (query.executeStep()) {
        uint32_t resFileId = query.getColumn("FileId");
        SQLite::Column keyColumn = query.getColumn("Key");
        KeyType resKey(static_cast<const char *>(keyColumn.getBlob()), keyColumn.getBytes());

        if (isKeyPrefix && !resKey.startsWith(key))
            break;

        TableDatabase::QueryResult queryResult = cb(resFileId, resKey);
        if (queryResult == TableDatabase::Stop)
            break;
    }
}

//
// Delete entries from table database whois FileId matches the caller-specified
//
void TableDatabase::deleteUnit(uint32_t fileId)
{
on_retry:
    try {
        TableDatabaseTxn txn(this->mDatabase);
        TableDatabaseDeleteUnit(this->mDatabase, fileId);
        txn.commit();
    } catch (SQLite::Exception &e) {
        if (e.getErrorCode() == SQLITE_BUSY)
            goto on_retry;
        throw TableDatabaseException(e);
    }
}

//
// Update all entries in table database whose FileId is the caller-specified FileId
//
void TableDatabase::updateUnit(uint32_t fileId, const UpdateUnitArgs &args)
{
on_retry:
    try {
        TableDatabaseTxn txn(this->mDatabase);

        // Delete entries related to @FileId
        TableDatabaseDeleteUnit(this->mDatabase, fileId);

        // Insert new entries to the database
        TableDatabaseInsert(this->mDatabase, Project::SymbolNames, fileId, *args.symbolNames);
        TableDatabaseInsert(this->mDatabase, Project::Targets, fileId, *args.targets);
        TableDatabaseInsert(this->mDatabase, Project::Usrs, fileId, *args.usrs);

        txn.commit();
    } catch (SQLite::Exception &e) {
        if (e.getErrorCode() == SQLITE_BUSY)
            goto on_retry;
        throw TableDatabaseException(e);
    }
}

//
// Query database to fetch key:value pair from specified tables
//
void TableDatabase::queryTargets(uint32_t fileId, const String &keyTarget, bool isKeyPrefix,
                                 std::function<QueryResult(uint32_t fileId, const String &key)> cb)
{
on_retry:
    try {
        TableDatabaseQuery<String>(this->mDatabase, Project::Targets, fileId, keyTarget, isKeyPrefix, cb);
    } catch (SQLite::Exception &e) {
        if (e.getErrorCode() == SQLITE_BUSY)
            goto on_retry;
        throw TableDatabaseException(e);
    }
}

void TableDatabase::querySymbolNames(uint32_t fileId, const String &keySymbolName, bool isKeyPrefix,
                                     std::function<QueryResult(uint32_t fileId, const String &key)> cb)
{
on_retry:
    try {
        TableDatabaseQuery<String>(this->mDatabase, Project::SymbolNames, fileId, keySymbolName, isKeyPrefix, cb);
    } catch (SQLite::Exception &e) {
        if (e.getErrorCode() == SQLITE_BUSY)
            goto on_retry;
        throw TableDatabaseException(e);
    }
}

void TableDatabase::queryUsrs(uint32_t fileId, const String &keyUsrs, bool isKeyPrefix,
                              std::function<QueryResult(uint32_t fileId, const String &key)> cb)
{
on_retry:
    try {
        TableDatabaseQuery<String>(this->mDatabase, Project::Usrs, fileId, keyUsrs, isKeyPrefix, cb);
    } catch (SQLite::Exception &e) {
        if (e.getErrorCode() == SQLITE_BUSY)
            goto on_retry;
        throw TableDatabaseException(e);
    }
}
