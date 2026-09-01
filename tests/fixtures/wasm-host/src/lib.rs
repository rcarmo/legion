use extism_pdk::*;

#[host_fn("extism:host/user")]
extern "ExtismHost" {
    fn log(message: String);
    fn read(key: String) -> String;
    fn write(key: String, value: String);
    fn budget(requested: u64) -> u64;
}

#[plugin_fn]
pub unsafe fn run(_input: String) -> FnResult<String> {
    unsafe {
        log("host fixture".into())?;
        let before = read("/value".into())?;
        write("/value".into(), "after".into())?;
        let granted = budget(12)?;
        Ok(serde_json::json!({"before": before, "granted": granted}).to_string())
    }
}
