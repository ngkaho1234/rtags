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

#ifndef CursorInfo_h
#define CursorInfo_h

#include <rct/String.h>
#include "Location.h"
#include <rct/Path.h>
#include <rct/Log.h>
#include <rct/List.h>
#include <clang-c/Index.h>
#include "RTags.h"
#include <rct/DB.h>

// class CursorInfo;
// typedef DB<Location, std::shared_ptr<CursorInfo> > SymbolMap; // duplicated from RTags.h
// typedef Map<Location, std::shared_ptr<CursorInfo> > SymbolMapMemory; // duplicated from RTags.h
class CursorInfo
{
public:
    CursorInfo()
        : symbolLength(0), kind(CXCursor_FirstInvalid), type(CXType_Invalid), enumValue(0),
          startLine(-1), startColumn(-1), endLine(-1), endColumn(-1)
    {}

    void clear()
    {
        symbolLength = 0;
        kind = CXCursor_FirstInvalid;
        type = CXType_Invalid;
        enumValue = 0;
        symbolName.clear();
#ifndef RTAGS_RP
        targets.clear();
        references.clear();
        project.reset();
        location.clear();
#endif
    }

    // these are used for the values in TargetsMapMemory/TargetsMap
    enum { DefinitionBit = 0x1000 };
    static CXCursorKind targetsValueKind(uint16_t val) { return static_cast<CXCursorKind>(val & ~DefinitionBit); }
    static bool targetsValueIsDefinition(uint16_t val) { return val & DefinitionBit; }
    static uint16_t createTargetsValue(CXCursorKind kind, bool definition) { return (kind | (definition ? DefinitionBit : 0)); }

    String kindSpelling() const { return kindSpelling(kind); }
    static String kindSpelling(uint16_t kind);

    String displayName() const;

    static int targetRank(CXCursorKind kind);

    bool isValid() const
    {
        return !isEmpty();
    }

    bool isNull() const
    {
        return isEmpty();
    }

#ifndef RTAGS_RP
    std::shared_ptr<CursorInfo> bestTarget() const;
    SymbolMapMemory targetInfos() const;
    SymbolMapMemory referenceInfos() const;
    SymbolMapMemory callers() const;
    SymbolMapMemory allReferences() const;
    SymbolMapMemory virtuals() const;

    static std::shared_ptr<CursorInfo> findCursorInfo(const std::shared_ptr<Project> &project,
                                                      const Location &location,
                                                      std::unique_ptr<SymbolMap::Iterator> *iterator = 0);
    std::shared_ptr<CursorInfo> copy() const;
    std::shared_ptr<CursorInfo> populate(const Location &location, const std::shared_ptr<Project> &project) const;
#endif

    bool isClass() const
    {
        switch (kind) {
        case CXCursor_ClassDecl:
        case CXCursor_ClassTemplate:
        case CXCursor_StructDecl:
            return true;
        default:
            break;
        }
        return false;
    }

    inline bool isDefinition() const
    {
        return kind == CXCursor_EnumConstantDecl || definition;
    }

    bool isEmpty() const
    {
        return !symbolLength;
    }

    template <typename T>
    static inline void serialize(T &s, const SymbolMapMemory &t);
    template <typename T>
    static inline void deserialize(T &s, SymbolMapMemory &t);

    enum Flag {
        IgnoreTargets = 0x1,
        IgnoreReferences = 0x2,
        DefaultFlags = 0x0
    };
    String toString(unsigned cursorInfoFlags = DefaultFlags, unsigned keyFlags = 0) const;
    uint16_t symbolLength; // this is just the symbol name length e.g. foo => 3
    String symbolName; // this is fully qualified Foobar::Barfoo::foo
    uint16_t kind;
    CXTypeKind type;
    union {
        bool definition;
        int64_t enumValue; // only used if type == CXCursor_EnumConstantDecl
    };
#ifndef RTAGS_RP
    Set<Location> references;
    Map<Location, uint16_t> targets;
#endif
    int startLine, startColumn, endLine, endColumn;


#ifndef RTAGS_RP
    // Not stored in DB
    Location location;
#endif
private:
#ifndef RTAGS_RP
    enum Mode {
        ClassRefs,
        VirtualRefs,
        NormalRefs
    };
    std::shared_ptr<Project> project;

    static void allImpl(const std::shared_ptr<CursorInfo> &info, SymbolMapMemory &out, Mode mode, unsigned kind);
#endif
    static bool isReference(unsigned int kind);
};

template <> inline Serializer &operator<<(Serializer &s, const CursorInfo &t)
{
#ifndef RTAGS_RP
    assert(t.references.isEmpty());
    assert(t.targets.isEmpty());
#endif
    s << t.symbolLength << t.symbolName << static_cast<int>(t.kind) << static_cast<int>(t.type)
      << t.enumValue << t.startLine << t.startColumn << t.endLine << t.endColumn;
    return s;
}

template <> inline Deserializer &operator>>(Deserializer &s, CursorInfo &t)
{
    int kind, type;
    s >> t.symbolLength >> t.symbolName >> kind >> type >> t.enumValue
      >> t.startLine >> t.startColumn >> t.endLine >> t.endColumn;
    t.kind = static_cast<CXCursorKind>(kind);
    t.type = static_cast<CXTypeKind>(type);
    return s;
}

template <typename T>
inline void CursorInfo::serialize(T &s, const SymbolMapMemory &t)
{
    const uint32_t size = t.size();
    s << size;
    for (const auto &it : t)
        s << it.first << *it.second;
}

template <typename T>
inline void CursorInfo::deserialize(T &s, SymbolMapMemory &t)
{
    uint32_t size;
    s >> size;
    t.clear();
    while (size--) {
        Location location;
        s >> location;
        std::shared_ptr<CursorInfo> &ci = t[location];
        ci = std::make_shared<CursorInfo>();
        s >> *ci;
    }
}

inline Log operator<<(Log log, const CursorInfo &info)
{
    log << info.toString();
    return log;
}

#endif
