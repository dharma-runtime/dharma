use crate::DharmaError;

pub fn project(args: &[&str]) -> Result<(), DharmaError> {
    match args {
        ["rebuild", ..] => Err(DharmaError::Validation(
            "projection runtime not wired yet; use DHL projections and dh serve".to_string(),
        )),
        ["watch", ..] => Err(DharmaError::Validation(
            "projection runtime not wired yet; use DHL projections and dh serve".to_string(),
        )),
        _ => {
            print_usage();
            Ok(())
        }
    }
}

fn print_usage() {
    println!("Usage:");
    println!("  dh project rebuild");
    println!("  dh project watch");
}
