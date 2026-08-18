dependencies {
    // The middle of the chain. web names data-store and has never heard of
    // core, and a change to core still has to rerun web.
    api(projects.libs.core)
}
