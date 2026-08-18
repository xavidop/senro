def web_bundle(name, deps):
    native.filegroup(name = name, srcs = deps)
