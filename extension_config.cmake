# This file is included by DuckDB's build system. It specifies which extension to load

# Allow duplicate TableCatalogEntry::Name when linking extension with libduckdb_static (C++17 inline constexpr).
# No leading/trailing space (CMP0004). Set both per-target (DUCKDB_EXTRA_LINK_FLAGS) and global so libduckdb.so gets it.
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
	set(DUCKDB_EXTRA_LINK_FLAGS "-Wl,--allow-multiple-definition" CACHE STRING "" FORCE)
	if(CMAKE_EXE_LINKER_FLAGS)
		set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--allow-multiple-definition" CACHE STRING "" FORCE)
	else()
		set(CMAKE_EXE_LINKER_FLAGS "-Wl,--allow-multiple-definition" CACHE STRING "" FORCE)
	endif()
	if(CMAKE_SHARED_LINKER_FLAGS)
		set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--allow-multiple-definition" CACHE STRING "" FORCE)
	else()
		set(CMAKE_SHARED_LINKER_FLAGS "-Wl,--allow-multiple-definition" CACHE STRING "" FORCE)
	endif()
endif()

# Extension from this repo
duckdb_extension_load(duckherder
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)