# Code Formatting Guide

## Quick Start

Format all files (CMake + Protobuf):
```bash
make format-all
```

## What Gets Formatted

### ✅ CMake Files
- `CMakeLists.txt`
- Tool: `cmake-format`
- Install: `pip install cmake-format`

### ✅ Protobuf Files
- `src/proto/*.proto`
- Tool: `buf format`
- Installed: ✅ `/usr/local/bin/buf`

## Before Committing

Always run:
```bash
make format-all
```

This ensures consistent code style across the project.

## Tools Installed

1. **buf** (v1.28.1) - Protobuf formatter and linter
   - Location: `/usr/local/bin/buf`
   - Config: `buf.yaml`
   - Format: `buf format -w src/proto/`
   - Lint: `buf lint src/proto/`

2. **cmake-format** (optional)
   - Install: `pip install cmake-format`
   - Format: `cmake-format -i CMakeLists.txt`

## Configuration Files

- `buf.yaml` - Buf linting and breaking change rules
- `buf.gen.yaml` - Code generation config (reference)
- `.clang-format-proto` - Protobuf style config (reference)

## CI Integration (Future)

Add to `.github/workflows/`:
```yaml
- name: Check formatting
  run: |
    make format-all
    git diff --exit-code
```

