// Biên dịch lại khi migrations thay đổi (`sqlx::migrate!` nhúng file lúc compile).
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
