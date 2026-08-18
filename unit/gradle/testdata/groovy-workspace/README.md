# acme

A Groovy-DSL Gradle multi-project build, used as a fixture by unit/gradle.

    :libs:core  <-  :libs:store  <-  :apps:web
    :libs:testkit  <-  :apps:web            (a test-only dependency)
    :tools:codegen                          (projectDir is build-tools/codegen)
