# Look for the header file.
find_path(LibDB_CXX_INCLUDE NAMES db_cxx.h
  PATHS $ENV{LibDB_CXX_ROOT}/include /usr/local/include /usr/include
  DOC "Path in which the file db.h is located." )

# Look for the library.
find_library(LibDB_CXX_LIBRARY NAMES db_cxx
  PATHS $ENV{LibDB_CXX_ROOT}/lib /usr/lib /usr/local/lib
  DOC "Path to Berkeley DB library." )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibDB_CXX DEFAULT_MSG LibDB_CXX_INCLUDE LibDB_CXX_LIBRARY)

if(LIBDB_CXX_FOUND)
  message(STATUS "Found LibDB_CXX (include: ${LibDB_CXX_INCLUDE}, library: ${LibDB_CXX_LIBRARY})")
  set(LibDB_CXX_INCLUDES ${LibDB_CXX_INCLUDE})
  set(LibDB_CXX_LIBRARIES ${LibDB_CXX_LIBRARY})
  mark_as_advanced(LibDB_CXX_INCLUDE LibDB_CXX_LIBRARY)
endif()
