#include <list>

#include "TableDatabase.h"
#include "Project.h"

static constexpr auto DatabaseNames = {
    TableDatabase::SymbolNames,
    TableDatabase::Targets,
    TableDatabase::Usrs,
};

//
// The schema of database primary key is as follows:
// FileID(uint32_t):Blob
//
// and the content of database secondary key is the content of the blob field of primary key
//

// Set unix mode of the table database to 0644 upon creation.
// 0644 stands for owner:rw- group:r--, other:r--.
static constexpr int TABLEDATABASE_MODE = 0644;

//
// This is a callback routine which return the content of secondary key to its
// caller.
//
static int TableDatabaseGetSecondary(Db *, const Dbt *key, const Dbt *, Dbt *result)
{
    result->set_data(static_cast<char *>(key->get_data()) + sizeof(uint32_t));
    result->set_size(key->get_size() - sizeof(uint32_t));
    return 0;
}

//
// Table database open method
//
TableDatabase::TableDatabase(const Path &envPath) try
{
    mDatabaseEnv.reset(new DbEnv(0));
    mDatabaseEnv->open(envPath.c_str(), DB_INIT_LOCK|DB_INIT_MPOOL|DB_INIT_TXN|DB_INIT_LOG|DB_RECOVER|DB_CREATE, 0644);
} catch (DbException &e) {
    TableDatabaseException tableDbException(e.get_errno(), e.what());
    throw tableDbException;
}

TableDatabase::~TableDatabase()
{
    for (auto type : DatabaseNames) {
        mDatabase[type].reset();
        mSecondaryDatabase[type].reset();
    }
    mDatabaseEnv.reset();
}

int TableDatabase::open(const Path &dbPath)
{
    int ret = 0;
    DbTxn *txn = NULL;

    try {
        for (auto type : DatabaseNames) {
            String databaseName = fileMapName(type);
            String primaryDatabaseName = databaseName + ".primary";
            String secondaryDatabaseName = databaseName + ".secondary";
            mDatabase[type].reset(new Db(mDatabaseEnv.get(), 0));
            ret = mDatabase[type]->open(txn, dbPath.c_str(), primaryDatabaseName.c_str(), DB_BTREE,
                                        DB_CREATE, TABLEDATABASE_MODE);
            if (ret)
                goto on_error;
            mSecondaryDatabase[type].reset(new Db(mDatabaseEnv.get(), 0));
            mSecondaryDatabase[type]->set_flags(DB_DUPSORT);
            ret = mSecondaryDatabase[type]->open(txn, dbPath.c_str(), secondaryDatabaseName.c_str(), DB_BTREE,
                                                 DB_CREATE, TABLEDATABASE_MODE);
            if (ret)
                goto on_error;
            ret = mDatabase[type]->associate(txn, mSecondaryDatabase[type].get(), TableDatabaseGetSecondary, 0);
            if (ret)
                goto on_error;
        }
    } catch (DbException &e) {
        TableDatabaseException tableDbException(e.get_errno(), e.what());
        // Do cleanup in case of errors.
        for (auto type : DatabaseNames) {
            mDatabase[type].reset();
            mDatabase[type].reset();
        }
        throw tableDbException;
    }
    return 0;
on_error:
    // Do cleanup in case of errors.
    for (auto type : DatabaseNames) {
        mDatabase[type].reset();
        mDatabase[type].reset();
    }
    return ret;
}

//
// Database query routines
//
// XXX: For queries on secondary database we probably should not allow the callback routine to
// do any modifications on the database
//
static int TableDatabaseQuery(DbTxn *txn, Db *secondaryDatabase,
                              const Blob &key, bool isKeyPrefix,
                              std::function<TableDatabase::QueryResult(uint32_t fileId, const Blob &key, const Blob &value)> cb)
{
    int ret;
    Dbc *cursor;
    Blob firstSkey;
    Dbt skeyDbt, pkeyDbt, valueDbt;

    //
    // Assemble a blob key that is in the format of secondary database's key type
    // so that it could later be used as prefix/absolute matching by the
    // built-in lexicographical compare function.
    //
    // NOTE: The key format of the secondary database's key type is simply
    // the primary key type without FileId field.
    //
    firstSkey.append(key);
    skeyDbt.set_data(&firstSkey[0]);
    skeyDbt.set_size((u_int32_t)firstSkey.size());

    // Obtain a database cursor from class Db
    secondaryDatabase->cursor(txn, &cursor, 0);
    if (!(ret = cursor->pget(&skeyDbt, &pkeyDbt, &valueDbt, DB_SET_RANGE))) {
        do {
            // Obtain the full-format key and value from the current entry
            Blob skeyBlob(static_cast<char *>(skeyDbt.get_data()), skeyDbt.get_size());
            Blob pkeyBlob(static_cast<char *>(pkeyDbt.get_data()), pkeyDbt.get_size());
            Blob valueBlob(static_cast<char *>(valueDbt.get_data()), valueDbt.get_size());

            // Do a key comparison
            if (isKeyPrefix) {
                if (!skeyBlob.startsWith(firstSkey))
                    break;
            } else {
                if (skeyBlob.compare(firstSkey))
                    break;
            }

            uint32_t fileId = *reinterpret_cast<const uint32_t *>(pkeyBlob.data());
            TableDatabase::QueryResult queryResult = cb(fileId, skeyBlob, valueBlob);
            if (queryResult == TableDatabase::Stop)
                break;
        } while (!(ret = cursor->pget(&skeyDbt, &pkeyDbt, &valueDbt, DB_NEXT)));
    }

    cursor->close();
    // Since we don't use DB_CURRENT flag in Db::get() routine, the return value should
    // never be DB_KEYEMPTY
    assert(ret != DB_KEYEMPTY);
    if (ret == DB_NOTFOUND)
        ret = 0;
    return ret;
}
static int TableDatabaseQuery(DbTxn *txn, Db *database,
                              uint32_t fileId, const Blob &key, bool isKeyPrefix,
                              std::function<TableDatabase::QueryResult(uint32_t fileId, const Blob &key, const Blob &value)> cb)
{
    int ret;
    Dbc *cursor;
    Blob firstPkey;
    Dbt pkeyDbt, valueDbt;

    //
    // Assemble a blob key that is in the format of primary database's key type
    // so that it could later be used as prefix/absolute matching by the
    // built-in lexicographical compare function.
    //
    firstPkey.append(reinterpret_cast<char *>(&fileId), sizeof(uint32_t));
    firstPkey.append(key);
    pkeyDbt.set_data(&firstPkey[0]);
    pkeyDbt.set_size((u_int32_t)firstPkey.size());

    // Obtain a database cursor from class Db
    database->cursor(txn, &cursor, 0);
    if (!(ret = cursor->get(&pkeyDbt, &valueDbt, DB_SET_RANGE))) {
        do {
            // Obtain the full-format key and value from the current entry
            Blob pkeyBlob(static_cast<char *>(pkeyDbt.get_data()), pkeyDbt.get_size());
            Blob valueBlob(static_cast<char *>(valueDbt.get_data()), valueDbt.get_size());
            // Do a key comparison
            if (isKeyPrefix) {
                if (!pkeyBlob.startsWith(firstPkey))
                    break;
            } else {
                if (pkeyBlob.compare(firstPkey))
                    break;
            }

            // Extract the primary key content without fileId prefix
            Blob skeyBlob(pkeyBlob.data() + sizeof(uint32_t), pkeyBlob.size() - sizeof(uint32_t));
            TableDatabase::QueryResult queryResult = cb(fileId, skeyBlob, valueBlob);
            if (queryResult == TableDatabase::Stop)
                break;
        } while (!(ret = cursor->get(&pkeyDbt, &valueDbt, DB_NEXT)));
    }

    cursor->close();
    // Since we don't use DB_CURRENT flag in Db::get() routine, the return value should
    // never be DB_KEYEMPTY
    assert(ret != DB_KEYEMPTY);
    if (ret == DB_NOTFOUND)
        ret = 0;
    return ret;
}

//
// Delete all entries from table database whose FileId is the caller-specified FileId
//
static int TableDatabaseDeleteUnit(DbTxn *txn, Db *database, uint32_t fileId)
{
    int ret;
    Dbc *cursor;
    Blob firstPkey;
    Dbt pkeyDbt, valueDbt;

    //
    // Assemble a blob key that is in the format of primary database's key type
    // so that it could later be used as prefix/absolute matching by the
    // built-in lexicographical compare function.
    //
    firstPkey.append(reinterpret_cast<char *>(&fileId), sizeof(uint32_t));
    pkeyDbt.set_data(&firstPkey[0]);
    pkeyDbt.set_size((u_int32_t)firstPkey.size());

    // Obtain a database cursor from class Db
    database->cursor(txn, &cursor, 0);
    if (!(ret = cursor->get(&pkeyDbt, &valueDbt, DB_SET_RANGE))) {
        do {
            // Obtain the full-format key and value from the current entry
            Blob pkeyBlob(static_cast<char *>(pkeyDbt.get_data()), static_cast<uint32_t>(pkeyDbt.get_size()));
            // Do a key comparison
            if (!pkeyBlob.startsWith(firstPkey))
                break;

            ret = database->del(txn, &pkeyDbt, 0);
            if (ret && ret != DB_NOTFOUND)
                break;
            ret = 0;
        } while (!(ret = cursor->get(&pkeyDbt, &valueDbt, DB_NEXT)));
    }

    cursor->close();
    // Since we don't use DB_CURRENT flag in Db::get() routine, the return value should
    // never be DB_KEYEMPTY
    assert(ret != DB_KEYEMPTY);
    if (ret == DB_NOTFOUND)
        ret = 0;
    return ret;
}
int TableDatabase::DeleteUnitInternal(DbTxn *txn, uint32_t fileId)
{
    int ret;
    for (auto type : DatabaseNames) {
        ret = TableDatabaseDeleteUnit(txn, mDatabase[type].get(), fileId);
        if (ret)
            break;
    }
    return ret;
}

//
// Insert @map into the specified table
// Key has to be indexable type. Must be called within a transaction
//
static int TableDatabaseInsert(DbTxn *txn, Db *database,
                               uint32_t fileId,
                               const std::list<std::pair<Blob, Blob> > &itemsMap)
{
    int ret = 0;
    for (auto pair : itemsMap) {
        const Blob &keyBlob = pair.first, &valueBlob = pair.second;
        Blob keyDbtBlob, valueDbtBlob(valueBlob);
        Dbt keyDbt, valueDbt;

        // Prepend the key prepared by @iterfunc with a FileId
        keyDbtBlob.append(reinterpret_cast<char *>(&fileId), sizeof(uint32_t));
        keyDbtBlob.append(keyBlob);
        keyDbt.set_data(&keyDbtBlob[0]);
        keyDbt.set_size((u_int32_t)keyDbtBlob.size());
        valueDbt.set_data(&valueDbtBlob[0]);
        valueDbt.set_size((u_int32_t)valueDbtBlob.size());

        ret = database->put(txn, &keyDbt, &valueDbt, 0);
        if (ret)
            break;
    }
    return ret;
}

//
// Delete entries from table database whois FileId matches the caller-specified
//
int TableDatabase::deleteUnit(uint32_t fileId)
{
    int ret = 0;
    DbTxn *txn = NULL;

    try {
        ret = DeleteUnitInternal(txn, fileId);
        if (ret)
            goto on_error;
    } catch (DbException &e) {
        TableDatabaseException tableDbException(e.get_errno(), e.what());
        throw tableDbException;
    }

    return 0;
on_error:
    return ret;
}

void TableDatabaseMakeItemList(const FileMap<String, Set<Location> > &map, std::list<std::pair<Blob, Blob> > &list)
{
    for (unsigned int i = 0; i < map.count(); i++) {
        String key = map.keyAt(i);
        Set<Location> value = map.valueAt(i);
        Blob keyBlob(key.data(), key.size()), valueBlob;
        Serializer valueSerializer = getBlobSerializer(valueBlob);
        valueSerializer << value;
        list.push_back(std::make_pair(keyBlob, valueBlob));
    }
}

//
// Update all entries in table database whose FileId is the caller-specified FileId
//
// XXX: this currently lacks transaction handling... and that brings us MUCH BETTER
// error handling
//
int TableDatabase::updateUnit(uint32_t fileId, const UpdateUnitArgs &args)
{
    int ret;
    DbTxn *txn = NULL;

    try {
        // Delete entries related to @FileId
        ret = DeleteUnitInternal(txn, fileId);
        if (ret)
            goto on_error;

        //
        // Insert entries in datafiles into database.
        // We currently do not do that for symbols and tokens datafiles
        //
        {
            std::list<std::pair<Blob, Blob> > itemList;
            TableDatabaseMakeItemList(*args.symbolNames, itemList);
            ret = TableDatabaseInsert(txn, this->mDatabase[SymbolNames].get(), fileId, itemList);
            if (ret)
                goto on_error;
        }

        {
            std::list<std::pair<Blob, Blob> > itemList;
            TableDatabaseMakeItemList(*args.targets, itemList);
            ret = TableDatabaseInsert(txn, this->mDatabase[Targets].get(), fileId, itemList);
            if (ret)
                goto on_error;
        }

        {
            std::list<std::pair<Blob, Blob> > itemList;
            TableDatabaseMakeItemList(*args.usrs, itemList);
            ret = TableDatabaseInsert(txn, this->mDatabase[Usrs].get(), fileId, itemList);
            if (ret)
                goto on_error;
        }
    } catch (DbException &e) {
        TableDatabaseException tableDbException(e.get_errno(), e.what());
        throw tableDbException;
    }

    return 0;
on_error:
    return ret;
}

//
// Query database to fetch key:value pair from specified tables
//
int TableDatabase::queryTargets(const String &keyTarget, bool isKeyPrefix,
                                std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb)
{
    auto process = [this, &cb](uint32_t fileId, const Blob &key, const Blob &value) -> QueryResult {
        String target(key.data(), key.size());
        Set<Location> locations;
        Deserializer valueDeserializer = getBlobDeserializer(value);
        valueDeserializer >> locations;
        return cb(fileId, target, locations);
    };

    Blob keyBlob(keyTarget.data(), keyTarget.size());
    int ret = TableDatabaseQuery(NULL, mSecondaryDatabase[Targets].get(), keyBlob, isKeyPrefix, process);
    return ret;
}
int TableDatabase::queryTargets(uint32_t fileId, const String &keyTarget, bool isKeyPrefix,
                                std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb)
{
    auto process = [this, fileId, &cb](uint32_t, const Blob &key, const Blob &value) -> QueryResult {
        String target(key.data(), key.size());
        Set<Location> locations;
        Deserializer valueDeserializer = getBlobDeserializer(value);
        valueDeserializer >> locations;
        return cb(fileId, target, locations);
    };

    Blob keyBlob(keyTarget.data(), keyTarget.size());
    int ret = TableDatabaseQuery(NULL, mDatabase[Targets].get(), fileId, keyBlob, isKeyPrefix, process);
    return ret;
}

int TableDatabase::querySymbolNames(const String &keySymbolName, bool isKeyPrefix,
                                    std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb)
{
    auto process = [this, &cb](uint32_t fileId, const Blob &key, const Blob &value) -> QueryResult {
        String symbolName(key.data(), key.size());
        Set<Location> locations;
        Deserializer valueDeserializer = getBlobDeserializer(value);
        valueDeserializer >> locations;
        return cb(fileId, symbolName, locations);
    };

    Blob keyBlob(keySymbolName.data(), keySymbolName.size());
    int ret = TableDatabaseQuery(NULL, mSecondaryDatabase[SymbolNames].get(), keyBlob, isKeyPrefix, process);
    return ret;
}
int TableDatabase::querySymbolNames(uint32_t fileId, const String &keySymbolName, bool isKeyPrefix,
                                    std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb)
{
    auto process = [this, fileId, &cb](uint32_t, const Blob &key, const Blob &value) -> QueryResult {
        String symbolName(key.data(), key.size());
        Set<Location> locations;
        Deserializer valueDeserializer = getBlobDeserializer(value);
        valueDeserializer >> locations;
        return cb(fileId, symbolName, locations);
    };

    Blob keyBlob(keySymbolName.data(), keySymbolName.size());
    int ret = TableDatabaseQuery(NULL, mDatabase[SymbolNames].get(), fileId, keyBlob, isKeyPrefix, process);
    return ret;
}

int TableDatabase::queryUsrs(const String &keyUsrs, bool isKeyPrefix,
                             std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb)
{
    auto process = [this, &cb](uint32_t fileId, const Blob &key, const Blob &value) -> QueryResult {
        String usrs(key.data(), key.size());
        Set<Location> locations;
        Deserializer valueDeserializer = getBlobDeserializer(value);
        valueDeserializer >> locations;
        return cb(fileId, usrs, locations);
    };

    Blob keyBlob(keyUsrs.data(), keyUsrs.size());
    int ret = TableDatabaseQuery(NULL, mSecondaryDatabase[Usrs].get(), keyBlob, isKeyPrefix, process);
    return ret;
}
int TableDatabase::queryUsrs(uint32_t fileId, const String &keyUsrs, bool isKeyPrefix,
                             std::function<QueryResult(uint32_t fileId, const String &key, const Set<Location> &value)> cb)
{
    auto process = [this, fileId, &cb](uint32_t, const Blob &key, const Blob &value) -> QueryResult {
        String usrs(key.data(), key.size());
        Set<Location> locations;
        Deserializer valueDeserializer = getBlobDeserializer(value);
        valueDeserializer >> locations;
        return cb(fileId, usrs, locations);
    };

    Blob keyBlob(keyUsrs.data(), keyUsrs.size());
    int ret = TableDatabaseQuery(NULL, mDatabase[Usrs].get(), fileId, keyBlob, isKeyPrefix, process);
    return ret;
}
