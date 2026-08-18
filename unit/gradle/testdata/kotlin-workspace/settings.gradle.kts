// A Kotlin-DSL settings file, declarative throughout. Validated with
// `gradle -q projects`.
pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.PREFER_SETTINGS)
    repositories {
        mavenCentral()
    }
}

// Type-safe project accessors, which is what lets a build script write
// `projects.libs.dataStore` instead of `project(":libs:data-store")`.
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

rootProject.name = "acme-kt"

include(":apps:web")

include(
    ":libs:core",
    ":libs:data-store",
)

include(":libs:testkit")
