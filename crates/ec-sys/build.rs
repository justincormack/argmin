fn main() {
    let lib = pkg_config::Config::new()
        .atleast_version("2.0")
        .probe("libisal")
        .expect("libisal not found; install intel-isa-l or set PKG_CONFIG_PATH");

    for path in &lib.include_paths {
        println!("cargo:include={}", path.display());
    }
}
