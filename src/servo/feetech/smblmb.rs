use std::f64::consts::PI;
use std::io::{Error as IoError, ErrorKind};
use std::time::Duration;

use crate::modbus_protocol::ModbusProtocolHandler;
use crate::Result;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3_stub_gen::derive::*;

/// SMBLMB register addresses (Modbus holding registers).
#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub enum SmblmbRegister {
    // INFO
    FirmwareMainVersionNo = 0,
    FirmwareSecondaryVersion = 1,
    FirmwareReleaseDateYear = 2,
    FirmwareReleaseDateMmdd = 3,
    // EPROM
    Id = 10,
    BaudRate = 11,
    ReturnDelayTime = 12,
    MinPositionLimit = 13,
    MaxPositionLimit = 14,
    PositionOffsetValue = 15,
    WorkMode = 16,
    PositionPGain = 17,
    PositionDGain = 18,
    PositionIGain = 19,
    VelocityPGain = 20,
    VelocityIGain = 21,
    // SRAM
    GoalPosition = 128,
    TorqueEnable = 129,
    GoalAcceleration = 130,
    GoalVelocity = 131,
    MaxTorqueLimit = 132,
    EpromLockSign = 133,
    ErrorReset = 134,
    // RONLY
    HardwareErrorStatus = 256,
    PresentPosition = 257,
    PresentVelocity = 258,
    PresentPwm = 259,
    PresentInputVoltage = 260,
    PresentTemperature = 261,
    MovingStatus = 262,
    PresentCurrent = 263,
    // DEFAULT
    MaxVelocityLimit = 386,
    MinVelocityLimit = 387,
    AccelerationLimit = 388,
    StartingTorque = 389,
    CwDeadBand = 390,
    CcwDeadBand = 391,
    SettingByte = 392,
    ProtectionSwitch = 393,
    LedAlarmCondition = 394,
    MaxTemperatureLimit = 395,
    MaxInputVoltage = 396,
    MinInputVoltage = 397,
    OverloadCurrent = 398,
    OvercurrentProtectionTime = 399,
    ProtectTorque = 400,
    OverloadTorque = 401,
    OverloadProtectionTime = 402,
    AngularResolution = 403,
    TorqueLimitDefaultValue = 404,
    AccelerationDefaultValue = 405,
    VelocityDefaultValue = 406,
}

pub struct SmblmbController {
    modbus: Option<ModbusProtocolHandler>,
}

impl Default for SmblmbController {
    fn default() -> Self {
        Self::new()
    }
}

impl SmblmbController {
    pub fn new() -> Self {
        Self { modbus: None }
    }

    pub fn with_modbus_rtu(
        self,
        serial_port: &str,
        baudrate: u32,
        timeout: Duration,
    ) -> Result<Self> {
        Ok(Self {
            modbus: Some(ModbusProtocolHandler::rtu(serial_port, baudrate, timeout)?),
        })
    }

    fn modbus_mut(&mut self) -> Result<&mut ModbusProtocolHandler> {
        self.modbus.as_mut().ok_or_else(|| {
            Box::new(IoError::new(
                ErrorKind::NotConnected,
                "SMBLMB controller is not connected; call with_modbus_rtu first",
            )) as Box<dyn std::error::Error>
        })
    }

    pub fn read_raw_data(&mut self, id: u8, addr: u16, length: u16) -> Result<Vec<u16>> {
        self.modbus_mut()?.read(id, addr, length)
    }

    pub fn write_raw_data(&mut self, id: u8, addr: u16, data: Vec<u16>) -> Result<()> {
        self.modbus_mut()?.write(id, addr, &data)
    }

    pub fn sync_read_raw_data(&mut self, ids: &[u8], addr: u16, length: u16) -> Result<Vec<Vec<u16>>> {
        self.modbus_mut()?.sync_read(ids, addr, length)
    }

    pub fn sync_write_raw_data(&mut self, ids: &[u8], addr: u16, data: &[Vec<u16>]) -> Result<()> {
        self.modbus_mut()?.sync_write(ids, addr, data)
    }

    pub fn read_register_raw(&mut self, id: u8, register: SmblmbRegister) -> Result<Vec<u16>> {
        self.read_raw_data(id, register as u16, 1)
    }

    pub fn write_register_raw(
        &mut self,
        id: u8,
        register: SmblmbRegister,
        value: u16,
    ) -> Result<()> {
        self.write_raw_data(id, register as u16, vec![value])
    }

    pub fn read_torque_enable(&mut self, id: u8) -> Result<Vec<bool>> {
        let val = self.read_raw_data(id, SmblmbRegister::TorqueEnable as u16, 1)?;
        Ok(vec![val[0] != 0])
    }

    pub fn write_torque_enable(&mut self, id: u8, value: bool) -> Result<()> {
        self.write_raw_data(id, SmblmbRegister::TorqueEnable as u16, vec![u16::from(value)])
    }

    pub fn sync_read_torque_enable(&mut self, ids: &[u8]) -> Result<Vec<bool>> {
        let vals = self.sync_read_raw_data(ids, SmblmbRegister::TorqueEnable as u16, 1)?;
        Ok(vals.iter().map(|v| v[0] != 0).collect())
    }

    pub fn sync_write_torque_enable(&mut self, ids: &[u8], values: &[bool]) -> Result<()> {
        if ids.len() != values.len() {
            return Err(Box::new(IoError::new(
                ErrorKind::InvalidInput,
                "ids and values must have same length",
            )));
        }
        let raw = values
            .iter()
            .map(|v| vec![u16::from(*v)])
            .collect::<Vec<Vec<u16>>>();
        self.sync_write_raw_data(ids, SmblmbRegister::TorqueEnable as u16, &raw)
    }

    pub fn read_present_position(&mut self, id: u8) -> Result<Vec<f64>> {
        let val = self.read_raw_data(id, SmblmbRegister::PresentPosition as u16, 1)?;
        Ok(vec![position_raw_to_radians(val[0])])
    }

    pub fn sync_read_present_position(&mut self, ids: &[u8]) -> Result<Vec<f64>> {
        let vals = self.sync_read_raw_data(ids, SmblmbRegister::PresentPosition as u16, 1)?;
        Ok(vals.iter().map(|v| position_raw_to_radians(v[0])).collect())
    }

    pub fn write_goal_position(&mut self, id: u8, value: f64) -> Result<()> {
        self.write_raw_data(
            id,
            SmblmbRegister::GoalPosition as u16,
            vec![position_radians_to_raw(value)],
        )
    }

    pub fn sync_write_goal_position(&mut self, ids: &[u8], values: &[f64]) -> Result<()> {
        if ids.len() != values.len() {
            return Err(Box::new(IoError::new(
                ErrorKind::InvalidInput,
                "ids and values must have same length",
            )));
        }
        let raw = values
            .iter()
            .map(|v| vec![position_radians_to_raw(*v)])
            .collect::<Vec<Vec<u16>>>();
        self.sync_write_raw_data(ids, SmblmbRegister::GoalPosition as u16, &raw)
    }

    pub fn write_goal_velocity(&mut self, id: u8, value: u16) -> Result<()> {
        self.write_raw_data(id, SmblmbRegister::GoalVelocity as u16, vec![value])
    }

    pub fn write_goal_acceleration(&mut self, id: u8, value: u16) -> Result<()> {
        self.write_raw_data(id, SmblmbRegister::GoalAcceleration as u16, vec![value])
    }

    pub fn write_max_torque_limit(&mut self, id: u8, value: u16) -> Result<()> {
        self.write_raw_data(id, SmblmbRegister::MaxTorqueLimit as u16, vec![value])
    }
}

fn position_raw_to_radians(raw: u16) -> f64 {
    let degrees = (raw as f64 / 4095.0) * 360.0 - 180.0;
    degrees.to_radians()
}

fn position_radians_to_raw(radians: f64) -> u16 {
    let clamped = radians.clamp(-PI, PI);
    let degrees = clamped.to_degrees();
    (((degrees + 180.0) / 360.0) * 4095.0).round() as u16
}

#[cfg(feature = "python")]
#[gen_stub_pyclass]
#[pyo3::pyclass(frozen)]
pub struct SmblmbPyController(std::sync::Mutex<SmblmbController>);

#[cfg(feature = "python")]
#[gen_stub_pymethods]
#[pymethods]
impl SmblmbPyController {
    #[new]
    pub fn new(serial_port: &str, baudrate: u32, timeout: f32) -> PyResult<Self> {
        let c = SmblmbController::new()
            .with_modbus_rtu(
                serial_port,
                baudrate,
                Duration::from_secs_f32(timeout),
            )
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        Ok(Self(std::sync::Mutex::new(c)))
    }

    pub fn read_raw_data(
        &self,
        py: Python,
        id: u8,
        addr: u16,
        length: u16,
    ) -> PyResult<Py<pyo3::types::PyList>> {
        let x = self
            .0
            .lock()
            .unwrap()
            .read_raw_data(id, addr, length)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let l = pyo3::types::PyList::new(py, x.clone())?;
        Ok(l.unbind())
    }

    pub fn write_raw_data(
        &self,
        id: u8,
        addr: u16,
        data: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        let data = data.extract::<Vec<u16>>()?;
        self.0
            .lock()
            .unwrap()
            .write_raw_data(id, addr, data)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    pub fn read_present_position(
        &self,
        py: Python,
        id: u8,
    ) -> PyResult<Py<pyo3::types::PyList>> {
        let x = self
            .0
            .lock()
            .unwrap()
            .read_present_position(id)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let l = pyo3::types::PyList::new(py, x.clone())?;
        Ok(l.unbind())
    }

    pub fn sync_read_present_position(
        &self,
        py: Python,
        ids: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<Py<pyo3::types::PyList>> {
        let ids = ids.extract::<Vec<u8>>()?;
        let x = self
            .0
            .lock()
            .unwrap()
            .sync_read_present_position(&ids)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let l = pyo3::types::PyList::new(py, x.clone())?;
        Ok(l.unbind())
    }

    pub fn write_goal_position(&self, id: u8, value: f64) -> PyResult<()> {
        self.0
            .lock()
            .unwrap()
            .write_goal_position(id, value)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    pub fn sync_write_goal_position(
        &self,
        ids: &Bound<'_, pyo3::types::PyList>,
        values: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        let ids = ids.extract::<Vec<u8>>()?;
        let values = values.extract::<Vec<f64>>()?;
        self.0
            .lock()
            .unwrap()
            .sync_write_goal_position(&ids, &values)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    pub fn write_torque_enable(&self, id: u8, value: bool) -> PyResult<()> {
        self.0
            .lock()
            .unwrap()
            .write_torque_enable(id, value)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    pub fn write_goal_velocity(&self, id: u8, value: u16) -> PyResult<()> {
        self.0
            .lock()
            .unwrap()
            .write_goal_velocity(id, value)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    pub fn write_goal_acceleration(&self, id: u8, value: u16) -> PyResult<()> {
        self.0
            .lock()
            .unwrap()
            .write_goal_acceleration(id, value)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    pub fn write_max_torque_limit(&self, id: u8, value: u16) -> PyResult<()> {
        self.0
            .lock()
            .unwrap()
            .write_max_torque_limit(id, value)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}
