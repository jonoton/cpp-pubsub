---
layout: default
---

[🏠 Home](./) | [Next: Basic Usage >](./basic-usage.html)
<hr>

# Getting Started

To use `cpp-pubsub`, simply include the `cpppubsub.hpp` header in your project. Since it is a header-only library, there is no separate compilation step required for the library itself.

## Integration

You can integrate `cpp-pubsub` into your project in several different ways, depending on your build system and preferences.

### 1. CMake: `FetchContent` (Recommended)

You can have CMake automatically download and integrate `cpp-pubsub` during configuration using `FetchContent`. This ensures you are always using a specific version and avoids manual submodule management.

```cmake
include(FetchContent)
FetchContent_Declare(
  cpppubsub
  GIT_REPOSITORY https://github.com/jonoton/cpp-pubsub.git
  GIT_TAG main # You can replace 'main' with a specific release tag
)
FetchContent_MakeAvailable(cpppubsub)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE cpppubsub::cpppubsub)
```

### 2. CMake: `add_subdirectory`

If you prefer to include `cpp-pubsub` as a Git submodule or just copy the repository directly into your project's vendor directory:

```bash
git submodule add https://github.com/jonoton/cpp-pubsub.git vendor/cpppubsub
```

Then in your `CMakeLists.txt`:

```cmake
add_subdirectory(vendor/cpppubsub)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE cpppubsub::cpppubsub)
```

### 3. Direct Header Inclusion

Since it's a single header, the simplest approach for small projects is to just copy `cpppubsub.hpp` directly into your source tree and include it.

```cpp
// main.cpp
#include "cpppubsub.hpp"

int main() {
    cpppubsub::PubSub broker;
    return 0;
}
```

Compile it directly (ensure you enable C++17 and link `pthread` on Linux):
```bash
g++ -std=c++17 -pthread main.cpp -o my_app
```

---
[🏠 Home](./) | [Next: Basic Usage >](./basic-usage.html)
