// The root project configures every subproject, so every project depends on
// it and a change here reruns all of them.
subprojects {
    apply(plugin = "java-library")
}
