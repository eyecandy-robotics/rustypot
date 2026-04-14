use std::error::Error;
use std::time::Duration;

use rustypot::servo::feetech::smblmb::SmblmbController;

fn main() -> Result<(), Box<dyn Error>> {
    let mut c = SmblmbController::new().with_modbus_rtu(
        "/dev/ttyUSB0",
        115_200,
        Duration::from_millis(500),
    )?;

    let id = 1;

    c.write_torque_enable(id, true)?;
    c.write_max_torque_limit(id, 1000)?;
    c.write_goal_acceleration(id, 100)?;
    c.write_goal_velocity(id, 500)?;
    c.write_goal_position(id, 0.0)?;

    let pos = c.read_present_position(id)?;
    println!("SMBLMB present position (rad): {:?}", pos);

    c.write_torque_enable(id, false)?;

    Ok(())
}
