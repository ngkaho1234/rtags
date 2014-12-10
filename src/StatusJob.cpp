/* This file is part of RTags.

RTags is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

RTags is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with RTags.  If not, see <http://www.gnu.org/licenses/>. */

#include "StatusJob.h"
#include "CursorInfo.h"
#include "RTags.h"
#include "Server.h"
#include <clang-c/Index.h>
#include "Project.h"
#include "CompilerManager.h"

const char *StatusJob::delimiter = "*********************************";
StatusJob::StatusJob(const std::shared_ptr<QueryMessage> &q, const std::shared_ptr<Project> &project)
    : QueryJob(q, WriteUnfiltered|QuietJob, project), query(q->query())
{
}

int StatusJob::execute()
{
    bool matched = false;
    const char *alternatives = "fileids|watchedpaths|dependencies|symbols|references|targets|symbollocations|symbolnames|sources|jobs|info|compilers";

    if (!strcasecmp(query.constData(), "fileids")) {
        matched = true;
        if (!write(delimiter) || !write("fileids") || !write(delimiter))
            return 1;
        const Hash<uint32_t, Path> paths = Location::idsToPaths();
        for (Hash<uint32_t, Path>::const_iterator it = paths.begin(); it != paths.end(); ++it) {
            if (!write<256>("  %u: %s", it->first, it->second.constData()))
                return 1;
        }
        if (isAborted())
            return 1;
    }

    std::shared_ptr<Project> proj = project();
    if (!proj) {
        if (!matched)
            write(alternatives);
        return matched ? 0 : 1;
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "watchedpaths")) {
        matched = true;
        if (!write(delimiter) || !write("watchedpaths") || !write(delimiter))
            return 1;
        Set<Path> watched = proj->watchedPaths();
        if (!write("Indexer"))
            return 1;
        for (Set<Path>::const_iterator it = watched.begin(); it != watched.end(); ++it) {
            if (!write<256>("  %s", it->constData()))
                return 1;
        }
        if (proj->fileManager) {
            if (!write("FileManager"))
                return 1;
            watched = proj->fileManager->watchedPaths();
            for (Set<Path>::const_iterator it = watched.begin(); it != watched.end(); ++it) {
                if (!write<256>("  %s", it->constData()))
                    return 1;
            }
        }
        if (isAborted())
            return 1;
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "dependencies")) {
        matched = true;
        const std::shared_ptr<DependencyMap> map = proj->dependencies();
        if (!write(delimiter) || !write("dependencies") || !write(delimiter))
            return 1;
        DependencyMapMemory depsReversed;

        for (auto it = map->createIterator(); it->isValid(); it->next()) {
            if (!write<256>("  %s (%d) is depended on by", Location::path(it->key()).constData(), it->key()))
                return 1;
            const Set<uint32_t> &deps = it->value();
            for (Set<uint32_t>::const_iterator dit = deps.begin(); dit != deps.end(); ++dit) {
                if (!write<256>("    %s (%d)", Location::path(*dit).constData(), *dit))
                    return 1;
                depsReversed[*dit].insert(it->key());
            }
            if (isAborted())
                return 1;
        }
        for (auto it = depsReversed.begin(); it != depsReversed.end(); ++it) {
            write<256>("  %s (%d) depends on", Location::path(it->first).constData(), it->first);
            const Set<uint32_t> &deps = it->second;
            for (Set<uint32_t>::const_iterator dit = deps.begin(); dit != deps.end(); ++dit) {
                if (!write<256>("    %s (%d)", Location::path(*dit).constData(), *dit))
                    return 1;
            }
            if (isAborted())
                return 1;
        }
    }
    const bool symbollocations = !strcasecmp(query.constData(), "symbollocations");
    if (query.isEmpty() || symbollocations || !strcasecmp(query.constData(), "symbols")) {
        matched = true;
        const std::shared_ptr<SymbolMap> map = proj->symbols();
        write(delimiter);
        write(symbollocations ? "symbollocations" : "symbols");
        write(delimiter);
        for (auto it = map->createIterator(); it->isValid(); it->next()) {
            const Location loc = it->key();
            write(loc);
            if (!symbollocations) {
                const std::shared_ptr<CursorInfo> ci = it->value();
                auto fixed = ci->populate(loc, proj);
                write(fixed);
                write("------------------------");
            }
            if (isAborted())
                return 1;
        }
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "symbolnames")) {
        matched = true;
        const std::shared_ptr<SymbolNameMap> map = proj->symbolNames();
        write(delimiter);
        write("symbolnames");
        write(delimiter);
        for (auto it = map->createIterator(); it->isValid(); it->next()) {
            write<128>("  %s", it->key().constData());
            const Set<Location> &locations = it->value();
            for (Set<Location>::const_iterator lit = locations.begin(); lit != locations.end(); ++lit) {
                const Location &loc = *lit;
                write<1024>("    %s", loc.key().constData());
            }
            if (isAborted())
                return 1;
        }
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "references")) {
        matched = true;
        const std::shared_ptr<ReferencesMap> map = proj->references();
        write(delimiter);
        write("references");
        write(delimiter);
        for (auto it = map->createIterator(); it->isValid(); it->next()) {
            write(it->key());
            const Set<Location> &locations = it->value();
            for (Set<Location>::const_iterator lit = locations.begin(); lit != locations.end(); ++lit) {
                const Location &loc = *lit;
                write<1024>("    %s", loc.key().constData());
            }
            if (isAborted())
                return 1;
        }
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "targets")) {
        matched = true;
        const std::shared_ptr<TargetsMap> map = proj->targets();
        write(delimiter);
        write("targets");
        write(delimiter);
        for (auto it = map->createIterator(); it->isValid(); it->next()) {
            write(it->key());
            const Map<Location, uint16_t> &locations = it->value();
            for (auto lit = locations.begin(); lit != locations.end(); ++lit) {
                const Location &loc = lit->first;
                write<1024>("    %s (%d/%d/%d)", loc.key(keyFlags()).constData(),
                            CursorInfo::targetsValueKind(lit->second),
                            CursorInfo::targetsValueIsDefinition(lit->second),
                            CursorInfo::targetRank(CursorInfo::targetsValueKind(lit->second)));
            }
            if (isAborted())
                return 1;
        }
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "sources")) {
        matched = true;
        const std::shared_ptr<SourceMap> map = proj->sources();
        if (!write(delimiter) || !write("sources") || !write(delimiter))
            return 1;
        for (auto it = map->createIterator(); it->isValid(); it->next()) {
            if (!write<512>("  %s: %s", it->value().sourceFile().constData(), it->value().toString().constData()))
                return 1;
        }
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "jobs")) {
        matched = true;
        if (!write(delimiter) || !write("jobs") || !write(delimiter))
            return 1;
        Server::instance()->dumpJobs(connection());
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "compilers")) {
        matched = true;
        if (!write(delimiter) || !write("compilers") || !write(delimiter))
            return 1;
        Source source;
        for (const Path &compiler : CompilerManager::compilers()) {
            source.compilerId = Location::insertFile(compiler);
            source.defines.clear();
            source.includePaths.clear();
            CompilerManager::applyToSource(source, true, true);
            write(compiler);
            write("  Defines:");
            for (const auto &it : source.defines)
                write<512>("    %s", it.toString().constData());
            write("  Includepaths:");
            for (const auto &it : source.includePaths)
                write<512>("    %s", it.toString().constData());
            write("");
        }
    }

    if (query.isEmpty() || !strcasecmp(query.constData(), "info")) {
        matched = true;
        if (!write(delimiter) || !write("info") || !write(delimiter))
            return 1;
        String out;
        Log log(&out);
#ifdef NDEBUG
        out << "Running a release build\n";
#else
        out << "Running a debug build\n";
#endif
        const Server::Options &opt = Server::instance()->options();
        out << "socketFile" << opt.socketFile << '\n'
            << "dataDir" << opt.dataDir << '\n'
            << "options" << String::format("0x%x\n", opt.options)
            << "jobCount" << opt.jobCount << '\n'
            << "unloadTimer" << opt.unloadTimer << '\n'
            << "rpVisitFileTimeout" << opt.rpVisitFileTimeout << '\n'
            << "rpIndexerMessageTimeout" << opt.rpIndexerMessageTimeout << '\n'
            << "rpConnectTimeout" << opt.rpConnectTimeout << '\n'
            << "rpConnectTimeout" << opt.rpConnectTimeout << '\n'
            << "syncThreshold" << opt.syncThreshold << '\n'
            << "threadStackSize" << opt.threadStackSize << '\n'
            << "defaultArguments" << opt.defaultArguments << '\n'
            << "includePaths" << opt.includePaths << '\n'
            << "defines" << opt.defines << '\n'
            << "ignoredCompilers" << opt.ignoredCompilers;
        write(out);
    }

    if (!matched) {
        write<256>("rc -s %s", alternatives);
        return 1;
    } else {
        return 0;
    }
}
