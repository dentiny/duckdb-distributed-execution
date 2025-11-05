# Contributing

## Did you find a bug?

* **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/dentiny/duckdb-distributed-execution/issues).
* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/dentiny/duckdb-distributed-execution/issues/new/choose). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## Did you write a patch that fixes a bug?

* Great!
* If possible, add a unit test case to make sure the issue does not occur again.
* Make sure you run the code formatter (`make format-all`).
* Open a new GitHub pull request with the patch.
* Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

## Pull Requests

* Do not commit/push directly to the main branch. Instead, create a fork and file a pull request.
* When maintaining a branch, merge frequently with the main.
* When maintaining a branch, submit pull requests to the main frequently.
* If you are working on a bigger issue try to split it up into several smaller issues.
* Please do not open "Draft" pull requests. Rather, use issues or discussion topics to discuss whatever needs discussing.
* We reserve full and final discretion over whether or not we will merge a pull request. Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

## Building

* Install `ccache` to improve compilation speed.
* To pull latest dependencies, run `git submodule update --init --recursive`.
* To build the project, run `CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make <debug>`.

## Testing

* To run all the SQL tests, run `make test` (or `make test_debug` for debug build binaries).
* To run all C++ tests, run `make test_unit` (or `test_debug_unit` for debug build binaries).

## Formatting

* Use tabs for indentation, spaces for alignment.
* Lines should not exceed 120 columns.
* `clang-format` enforce these rules automatically, use `make format-all` to run the formatter.

### DuckDB C++ Guidelines

* Do not use `malloc`, prefer the use of smart pointers. Keywords `new` and `delete` are a code smell.
* Strongly prefer the use of `unique_ptr` over `shared_ptr`, only use `shared_ptr` if you **absolutely** have to.
* Use `const` whenever possible.
* Do **not** import namespaces (e.g. `using std`).
* All functions in source files in the core (`src` directory) should be part of the `pgduckdb` namespace.
* When overriding a virtual method, avoid repeating virtual and always use `override` or `final`.
* Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, `uint` etc. Use `idx_t` instead of `size_t` for offsets/indices/counts of any kind.
* Prefer using references over pointers as arguments.
* Use `const` references for arguments of non-trivial objects (e.g. `std::vector`, ...).
* Use C++11 for loops when possible: `for (const auto& item : items) {...}`
* Use braces for indenting `if` statements and loops. Avoid single-line if statements and loops, especially nested ones.
* **Class Layout:** Start out with a `public` block containing the constructor and public variables, followed by a `public` block containing public methods of the class. After that follow any private functions and private variables. For example:
    ```cpp
    class MyClass {
    public:
    	MyClass();

    	int my_public_variable;

    public:
    	void MyFunction();

    private:
    	void MyPrivateFunction();

    private:
    	int my_private_variable;
    };
    ```
* Avoid [unnamed magic numbers](https://en.wikipedia.org/wiki/Magic_number_(programming)). Instead, use named variables that are stored in a `constexpr`.
* [Return early](https://medium.com/swlh/return-early-pattern-3d18a41bba8). Avoid deep nested branches.
* Do not include commented out code blocks in pull requests.

## Error Handling

* Use exceptions **only** when an error is encountered that terminates a query (e.g. parser error, table not found). Exceptions should only be used for **exceptional** situations. For regular errors that do not break the execution flow (e.g. errors you **expect** might occur) use a return value instead.
* Try to add test cases that trigger exceptions. If an exception cannot be easily triggered using a test case then it should probably be an assertion. This is not always true (e.g. out of memory errors are exceptions, but are very hard to trigger).
* Use `D_ASSERT` to assert. Use **assert** only when failing the assert means a programmer error. Assert should never be triggered by user input. Avoid code like `D_ASSERT(a > b + 3);` without comments or context.
* Assert liberally, but make it clear with comments next to the assert what went wrong when the assert is triggered.

## Naming Conventions

* Choose descriptive names. Avoid single-letter variable names.
* Files: lowercase separated by underscores, e.g., abstract_operator.cpp
* Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., BaseColumn
* Variables: lowercase separated by underscores, e.g., chunk_size
* Functions: CamelCase starting with uppercase letter, e.g., GetChunk
* Avoid `i`, `j`, etc. in **nested** loops. Prefer to use e.g. **column_idx**, **check_idx**. In a **non-nested** loop it is permissible to use **i** as iterator index.
* These rules are partially enforced by `clang-tidy`.

## Release process

Extension is released to [duckdb community extension](https://github.com/duckdb/community-extensions) periodically to pick up latest changes.

Before submitting a PR to the repo, we need to make sure extension builds and runs well in **ALL** platforms.

Use Linux (operating system), amd64 (ISA) and musl (lib) as an example,
```sh
ubuntu@hjiang-devbox-pg$ git clone git@github.com:duckdb/extension-ci-tools.git
ubuntu@hjiang-devbox-pg$ cd extension-ci-tools/docker/linux_amd64_musl
# Build docker image with the Dockerfile of a specific platform.
ubuntu@hjiang-devbox-pg$ docker build -t duckdb-ci-linux-amd64-musl  .
# Start docker container and build the extension.
ubuntu@hjiang-devbox-pg$ docker run -it duckdb-ci-linux-amd64-musl
# Inside of the container.
/duckdb_build_dir # git clone https://github.com/dentiny/duckdb-distributed-execution.git && cd duckdb-distributed-execution
/duckdb_build_dir/duckdb-distributed-execution # git submodule update --init --recursive
/duckdb_build_dir/duckdb-distributed-execution # CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make
```
See [link](https://github.com/duckdb/extension-ci-tools/tree/main/docker) for all required environments and docker files.

## Update duckdb version

Community extension should use latest released duckdb, see [thread](https://github.com/duckdb/community-extensions/pull/346#issuecomment-2780398504) for details.

Steps to update duckdb commit and version:
```sh
# Switch to the desired version
ubuntu@hjiang-devbox-pg$ cd duckdb && git checkout tags/v1.2.1
# Commit updated duckdb.
ubuntu@hjiang-devbox-pg$ cd - && git add duckdb
```
