add_executable(test_pubsub
    test_pubsub.cpp
)

target_link_libraries(test_pubsub
    PRIVATE
        cpppubsub::cpppubsub
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(test_pubsub)
