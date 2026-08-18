dependencies {
    // A type-safe accessor. ":libs:data-store" is rendered "libs.dataStore",
    // so reading this edge means mapping the camelCase back to the path.
    implementation(projects.libs.dataStore)

    // The explicit form, in the same file, because a real repository migrates
    // to accessors one line at a time.
    testImplementation(project(":libs:testkit"))
}
