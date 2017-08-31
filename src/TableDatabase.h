#ifndef TableDatabase_h
#define TableDatabase_h

#include <utility>
#include <memory>
#include <functional>
#include <db_cxx.h>

#include "RTags.h"
#include "FileMap.h"
#include "Token.h"

class TableDatabaseException
{
public:
    TableDatabaseException(int errorCode, const String &errorStr) : mErrorCode(errorCode), mErrorStr(errorStr) {}

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
    TableDatabase(const Path &envPath);
    ~TableDatabase();

    int open(const Path &dbPath);

    enum FileMapType {
        Symbols,
        SymbolNames,
        Targets,
        Usrs,
        Tokens
    };

    static const int NFileMapTypes = 5;
    static const char *fileMapName(FileMapType type)
    {
        switch (type) {
        case Symbols: return "symbols";
        case SymbolNames: return "symnames";
        case Targets: return "targets";
        case Usrs: return "usrs";
        case Tokens: return "tokens";
        }
        return 0;
    }

    struct UpdateUnitArgs {
        FileMap<Location, Symbol> *symbols;
        FileMap<String, Set<Location> > *targets;
        FileMap<String, Set<Location> > *usrs;
        FileMap<String, Set<Location> > *symbolNames;
        FileMap<uint32_t, Token> *tokens;
    };

    enum QueryResult {
        Stop,
        Continue,
    };

    int deleteUnit(uint32_t fileId);
    int updateUnit(uint32_t fileId, const UpdateUnitArgs &args);

    int querySymbols(const Location &keyLocation,
                     std::function<QueryResult(uint32_t fileId, const Location &key, const Symbol &value)> cb);
    int querySymbols(uint32_t fileId, const Location &keyLocation,
                     std::function<QueryResult(uint32_t fileId, const Location &key, const Symbol &value)> cb);

    int queryTargets(const String &keyTarget, bool isKeyPrefix,
                     std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb);
    int queryTargets(uint32_t fileId, const String &keyTarget, bool isKeyPrefix,
                     std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb);

    int querySymbolNames(const String &keySymbolName, bool isKeyPrefix,
                         std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb);
    int querySymbolNames(uint32_t fileId, const String &keySymbolName, bool isKeyPrefix,
                         std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb);

    int queryUsrs(const String &keyUsrs, bool isKeyPrefix,
                  std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb);
    int queryUsrs(uint32_t fileId, const String &keyUsrs, bool isKeyPrefix,
                  std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb);

    int queryToken(uint32_t keyTokenId,
                   std::function<QueryResult(uint32_t fileId, uint32_t key, const Token &value)> cb);
    int queryToken(uint32_t fileId, uint32_t keyTokenId,
                   std::function<QueryResult(uint32_t fileId, uint32_t key, const Token &value)> cb);

private:
    std::unique_ptr<DbEnv> mDatabaseEnv;
    std::unique_ptr<Db> mDatabase[NFileMapTypes];
    std::unique_ptr<Db> mSecondaryDatabase[NFileMapTypes];

    int DeleteUnitInternal(DbTxn *txn, uint32_t fileId);
};

#endif
