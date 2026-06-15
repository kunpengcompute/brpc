load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:local.bzl", "new_local_repository")


def _local_deps_impl(module_ctx):
    """Create local/git repositories that don't have their own MODULE.bazel.

    This is used to keep using @urma, @libboundscheck and @ubsocket when
    bzlmod is enabled, without modifying those external repositories.
    """

    new_local_repository( 
        name = "urma", 
        path = "/usr", 
        build_file = "//3rdparty/urma:BUILD.bazel", 
    )
    git_repository(
        name = "libboundscheck",
        remote = "https://atomgit.com/openeuler/libboundscheck.git",
        branch = "master",
        build_file = "//3rdparty/boundscheck:BUILD.bazel",
    )

    git_repository(
        name = "ubsocket",
        remote = "https://atomgit.com/openeuler/ubs-comm.git",
        commit = "63f5ce376b32e633ac9cdf0a716d8cef07a918e8",
    )

    # boringssl: keep exactly the same as WORKSPACE definition
    git_repository(
        name = "boringssl",  # 2021-05-01T12:26:01Z
        commit = "c00d7ca810e93780bd0c8ee4eea28f4f2ea4bcdc",  # fips-20210429
        remote = "https://github.com/google/boringssl",
    )


local_deps = module_extension(
    implementation = _local_deps_impl,
)
