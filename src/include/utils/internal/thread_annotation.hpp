#pragma once

// Enable Clang's thread-safety attributes when they are available. Other
// compilers erase the attributes while keeping the annotated types usable.
#if defined(__clang__) && !defined(SWIG)
#define DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(x) __attribute__((x))
#ifndef DUCKDB_THREAD_ANNOTATION_ENABLED
#define DUCKDB_THREAD_ANNOTATION_ENABLED 1
#endif
#else
#define DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(x)
#ifndef DUCKDB_THREAD_ANNOTATION_ENABLED
#define DUCKDB_THREAD_ANNOTATION_ENABLED 0
#endif
#endif

#define DUCKDB_CAPABILITY(x)     DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(capability(x))
#define DUCKDB_SCOPED_CAPABILITY DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(scoped_lockable)
