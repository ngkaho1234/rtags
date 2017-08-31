#ifndef TableDatabase_h
#define TableDatabase_h

#include <SQLiteCpp/SQLiteCpp.h>
#include <utility>
#include <pthread.h>

#include "rct/String.h"
#include "rct/Path.h"
#include "rct/SharedMemory.h"
#include "RTags.h"
#include "Token.h"

class TableDatabaseException
{
public:
    TableDatabaseException(int errorCode, String &errorStr) : mErrorCode(errorCode), mErrorStr(errorStr) {}

    TableDatabaseException(const SQLite::Exception &e) : mErrorCode(e.getErrorCode()), mErrorStr(e.getErrorStr()) {}

    inline int getErrorCode()
    {
        return mErrorCode;
    }

    inline String getErrorStr()
    {
        return mErrorStr;
    }

private:
    const int mErrorCode;
    String mErrorStr;
};

class TableDatabase {
public:
    TableDatabase(const Path &path, bool create = false);
    ~TableDatabase();

    static int TableDatabaseCommitCallback(void *arg, sqlite3 *, const char *databaseName, int numberFrames);

    struct UpdateUnitArgs {
        Map<String, Set<Location> > *targets;
        Map<String, Set<Location> > *usrs;
        Map<String, Set<Location> > *symbolNames;
    };

    enum QueryResult {
        Stop,
        Continue,
    };

    void deleteUnit(uint32_t fileId);
    void updateUnit(uint32_t fileId, const UpdateUnitArgs &args);

    void queryTargets(uint32_t fileId, const String &keyTarget, bool isKeyPrefix,
                      std::function<QueryResult(uint32_t fileId, const String &key)> cb);

    void querySymbolNames(uint32_t fileId, const String &keySymbolName, bool isKeyPrefix,
                          std::function<QueryResult(uint32_t fileId, const String &key)> cb);

    void queryUsrs(uint32_t fileId, const String &keyUsrs, bool isKeyPrefix,
                   std::function<QueryResult(uint32_t fileId, const String &key)> cb);

private:
    bool mCreated;
    SQLite::Database mDatabase;
};

#endif
