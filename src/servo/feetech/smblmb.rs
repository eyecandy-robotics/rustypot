use std::collections::{HashMap, VecDeque};
use std::f64::consts::PI;
use std::io::{Error as IoError, ErrorKind};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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

// =====================================================================
// Async (background-thread) controller
// =====================================================================
//
// Modbus RTU is fundamentally request/response per slave; with two servos
// at 115200 baud the synchronous loop tops out around ~70 Hz because every
// Python call blocks the caller on the wire. The controller below moves
// all bus I/O onto a dedicated worker thread so that:
//
//   * `sync_write_goal_position` only updates a shared command cache and
//     returns immediately,
//   * `sync_read_present_position` returns the most recent cached reading
//     (also non-blocking),
//   * the worker continuously alternates a goal-position sync write and
//     a present-position sync read, running the bus as fast as it can,
//   * occasional raw register writes (torque enable, gains, etc.) are
//     queued and flushed by the worker between hot operations,
//   * one-off raw reads execute on the worker via a small request/response
//     channel so they do not race the hot loop.
//
// The Python-facing API mirrors the synchronous controller for the
// methods used in the runtime hot loop, so it is a drop-in replacement.

type RawReadResult = std::result::Result<Vec<u16>, String>;

struct RawReadRequest {
    id: u8,
    addr: u16,
    length: u16,
    response: Arc<(Mutex<Option<RawReadResult>>, Condvar)>,
}

struct AsyncSharedState {
    polled_ids: Vec<u8>,
    id_to_index: HashMap<u8, usize>,
    goal_positions: Vec<f64>,
    goal_dirty: bool,
    present_positions: Vec<f64>,
    present_valid: bool,
    cycles: u64,
    last_cycle_at: Option<Instant>,
    last_cycle_dt_s: f64,
    raw_writes: VecDeque<(u8, u16, Vec<u16>)>,
    raw_reads: VecDeque<RawReadRequest>,
    last_error: Option<String>,
    stop: bool,
}

impl AsyncSharedState {
    fn new(polled_ids: Vec<u8>, initial_positions: Vec<f64>) -> Self {
        let id_to_index = polled_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i))
            .collect();
        Self {
            goal_positions: initial_positions.clone(),
            present_positions: initial_positions,
            id_to_index,
            polled_ids,
            goal_dirty: false,
            present_valid: true,
            cycles: 0,
            last_cycle_at: None,
            last_cycle_dt_s: 0.0,
            raw_writes: VecDeque::new(),
            raw_reads: VecDeque::new(),
            last_error: None,
            stop: false,
        }
    }
}

pub struct SmblmbAsyncController {
    state: Arc<Mutex<AsyncSharedState>>,
    cv: Arc<Condvar>,
    handle: Option<JoinHandle<()>>,
}

impl SmblmbAsyncController {
    pub fn start(
        serial_port: &str,
        baudrate: u32,
        timeout: Duration,
        motor_ids: Vec<u8>,
    ) -> Result<Self> {
        if motor_ids.is_empty() {
            return Err(Box::new(IoError::new(
                ErrorKind::InvalidInput,
                "SmblmbAsyncController requires at least one motor id",
            )));
        }
        let mut seen = HashMap::new();
        for &id in &motor_ids {
            if seen.insert(id, ()).is_some() {
                return Err(Box::new(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("SmblmbAsyncController motor_ids contains duplicate id {id}"),
                )));
            }
        }

        let mut ctrl = SmblmbController::new().with_modbus_rtu(serial_port, baudrate, timeout)?;

        // Seed the cache with one synchronous read so the first hot-loop
        // call sees a sane value rather than zeros.
        let initial = ctrl.sync_read_present_position(&motor_ids)?;

        let state = Arc::new(Mutex::new(AsyncSharedState::new(
            motor_ids.clone(),
            initial,
        )));
        let cv = Arc::new(Condvar::new());

        let state_for_worker = state.clone();
        let cv_for_worker = cv.clone();
        let handle = thread::Builder::new()
            .name("smblmb-async-worker".to_string())
            .spawn(move || async_worker_loop(ctrl, state_for_worker, cv_for_worker))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(Self {
            state,
            cv,
            handle: Some(handle),
        })
    }

    pub fn polled_ids(&self) -> Vec<u8> {
        self.state.lock().unwrap().polled_ids.clone()
    }

    pub fn set_goal_positions(&self, ids: &[u8], values: &[f64]) -> Result<()> {
        if ids.len() != values.len() {
            return Err(Box::new(IoError::new(
                ErrorKind::InvalidInput,
                "ids and values must have same length",
            )));
        }
        let mut s = self.state.lock().unwrap();
        for (&id, &v) in ids.iter().zip(values.iter()) {
            let idx = *s.id_to_index.get(&id).ok_or_else(|| {
                Box::new(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("motor id {id} not registered with async controller"),
                )) as Box<dyn std::error::Error>
            })?;
            s.goal_positions[idx] = v;
        }
        s.goal_dirty = true;
        self.cv.notify_one();
        Ok(())
    }

    pub fn get_present_positions(&self, ids: &[u8]) -> Result<Vec<f64>> {
        let s = self.state.lock().unwrap();
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            let idx = *s.id_to_index.get(&id).ok_or_else(|| {
                Box::new(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("motor id {id} not registered with async controller"),
                )) as Box<dyn std::error::Error>
            })?;
            out.push(s.present_positions[idx]);
        }
        Ok(out)
    }

    pub fn queue_raw_write(&self, id: u8, addr: u16, data: Vec<u16>) {
        let mut s = self.state.lock().unwrap();
        s.raw_writes.push_back((id, addr, data));
        self.cv.notify_one();
    }

    pub fn blocking_raw_read(
        &self,
        id: u8,
        addr: u16,
        length: u16,
        deadline: Option<Duration>,
    ) -> Result<Vec<u16>> {
        let response = Arc::new((Mutex::new(None::<RawReadResult>), Condvar::new()));
        {
            let mut s = self.state.lock().unwrap();
            s.raw_reads.push_back(RawReadRequest {
                id,
                addr,
                length,
                response: response.clone(),
            });
            self.cv.notify_one();
        }
        let (lock, cv) = &*response;
        let mut guard = lock.lock().unwrap();
        let deadline = deadline.unwrap_or_else(|| Duration::from_secs(2));
        let start = Instant::now();
        while guard.is_none() {
            let elapsed = start.elapsed();
            if elapsed >= deadline {
                return Err(Box::new(IoError::new(
                    ErrorKind::TimedOut,
                    "blocking raw read timed out waiting for worker",
                )));
            }
            let (g, _) = cv.wait_timeout(guard, deadline - elapsed).unwrap();
            guard = g;
        }
        match guard.take().unwrap() {
            Ok(v) => Ok(v),
            Err(e) => Err(Box::new(IoError::new(ErrorKind::Other, e))),
        }
    }

    pub fn cycles(&self) -> u64 {
        self.state.lock().unwrap().cycles
    }

    pub fn last_cycle_dt_s(&self) -> f64 {
        self.state.lock().unwrap().last_cycle_dt_s
    }

    pub fn take_last_error(&self) -> Option<String> {
        self.state.lock().unwrap().last_error.take()
    }

    pub fn stop(&mut self) {
        {
            let mut s = self.state.lock().unwrap();
            s.stop = true;
        }
        self.cv.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for SmblmbAsyncController {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop();
        }
    }
}

fn async_worker_loop(
    mut ctrl: SmblmbController,
    state: Arc<Mutex<AsyncSharedState>>,
    cv: Arc<Condvar>,
) {
    loop {
        // Snapshot work to do this cycle.
        let (polled_ids, goals, goal_dirty, raw_writes, raw_reads) = {
            let mut guard = state.lock().unwrap();
            // If there is genuinely nothing to do (no polled ids and no
            // queued ops) wait for a wake-up. With polled ids we never
            // wait so the loop runs the bus as fast as it can.
            while !guard.stop
                && guard.polled_ids.is_empty()
                && guard.raw_writes.is_empty()
                && guard.raw_reads.is_empty()
            {
                guard = cv.wait(guard).unwrap();
            }
            if guard.stop {
                // Drain any pending raw writes so things like a final
                // disable_torque from close() are not silently dropped.
                let final_writes = std::mem::take(&mut guard.raw_writes);
                drop(guard);
                for (id, addr, data) in final_writes {
                    let _ = ctrl.write_raw_data(id, addr, data);
                }
                break;
            }
            let goal_dirty = guard.goal_dirty;
            guard.goal_dirty = false;
            (
                guard.polled_ids.clone(),
                guard.goal_positions.clone(),
                goal_dirty,
                std::mem::take(&mut guard.raw_writes),
                std::mem::take(&mut guard.raw_reads),
            )
        };

        // Service one-shot raw reads first; callers are blocked on these.
        for req in raw_reads {
            let result = ctrl
                .read_raw_data(req.id, req.addr, req.length)
                .map_err(|e| e.to_string());
            let (lock, cv_resp) = &*req.response;
            let mut g = lock.lock().unwrap();
            *g = Some(result);
            cv_resp.notify_one();
        }

        // Flush raw writes (torque enable, gain registers, ...).
        for (id, addr, data) in raw_writes {
            if let Err(e) = ctrl.write_raw_data(id, addr, data) {
                record_error(&state, e.to_string());
            }
        }

        if polled_ids.is_empty() {
            continue;
        }

        // Hot path: write fresh goals (if any) then read positions.
        if goal_dirty && goals.len() == polled_ids.len() {
            if let Err(e) = ctrl.sync_write_goal_position(&polled_ids, &goals) {
                record_error(&state, e.to_string());
            }
        }

        match ctrl.sync_read_present_position(&polled_ids) {
            Ok(positions) => {
                let now = Instant::now();
                let mut guard = state.lock().unwrap();
                guard.present_positions = positions;
                guard.present_valid = true;
                guard.cycles = guard.cycles.wrapping_add(1);
                if let Some(prev) = guard.last_cycle_at {
                    guard.last_cycle_dt_s = now.duration_since(prev).as_secs_f64();
                }
                guard.last_cycle_at = Some(now);
            }
            Err(e) => record_error(&state, e.to_string()),
        }
    }
}

fn record_error(state: &Arc<Mutex<AsyncSharedState>>, msg: String) {
    let mut s = state.lock().unwrap();
    s.last_error = Some(msg);
}

#[cfg(feature = "python")]
#[gen_stub_pyclass]
#[pyo3::pyclass(frozen)]
pub struct SmblmbAsyncPyController(std::sync::Mutex<Option<SmblmbAsyncController>>);

#[cfg(feature = "python")]
impl SmblmbAsyncPyController {
    fn with_inner<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&SmblmbAsyncController) -> PyResult<R>,
    {
        let guard = self.0.lock().unwrap();
        match guard.as_ref() {
            Some(inner) => f(inner),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "SmblmbAsyncPyController is closed",
            )),
        }
    }
}

#[cfg(feature = "python")]
#[gen_stub_pymethods]
#[pymethods]
impl SmblmbAsyncPyController {
    #[new]
    pub fn new(
        serial_port: &str,
        baudrate: u32,
        timeout: f32,
        motor_ids: Vec<u8>,
    ) -> PyResult<Self> {
        let inner = SmblmbAsyncController::start(
            serial_port,
            baudrate,
            Duration::from_secs_f32(timeout),
            motor_ids,
        )
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(Self(std::sync::Mutex::new(Some(inner))))
    }

    /// Non-blocking. Updates the goal-position cache; the worker thread
    /// pushes it to the bus on its next cycle.
    pub fn sync_write_goal_position(
        &self,
        ids: &Bound<'_, pyo3::types::PyList>,
        values: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        let ids = ids.extract::<Vec<u8>>()?;
        let values = values.extract::<Vec<f64>>()?;
        self.with_inner(|inner| {
            inner
                .set_goal_positions(&ids, &values)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Convenience single-id form. Also non-blocking.
    pub fn write_goal_position(&self, id: u8, value: f64) -> PyResult<()> {
        self.with_inner(|inner| {
            inner
                .set_goal_positions(&[id], &[value])
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Non-blocking. Returns the latest cached present positions.
    pub fn sync_read_present_position(
        &self,
        py: Python,
        ids: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<Py<pyo3::types::PyList>> {
        let ids = ids.extract::<Vec<u8>>()?;
        let positions = self.with_inner(|inner| {
            inner
                .get_present_positions(&ids)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(pyo3::types::PyList::new(py, positions)?.unbind())
    }

    /// Non-blocking single-id read of cached present position.
    pub fn read_present_position(
        &self,
        py: Python,
        id: u8,
    ) -> PyResult<Py<pyo3::types::PyList>> {
        let positions = self.with_inner(|inner| {
            inner
                .get_present_positions(&[id])
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(pyo3::types::PyList::new(py, positions)?.unbind())
    }

    /// Queues a torque-enable write. Non-blocking.
    pub fn write_torque_enable(&self, id: u8, value: bool) -> PyResult<()> {
        self.with_inner(|inner| {
            inner.queue_raw_write(
                id,
                SmblmbRegister::TorqueEnable as u16,
                vec![u16::from(value)],
            );
            Ok(())
        })
    }

    /// Queues a torque-enable write per id. Non-blocking.
    pub fn sync_write_torque_enable(
        &self,
        ids: &Bound<'_, pyo3::types::PyList>,
        values: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        let ids = ids.extract::<Vec<u8>>()?;
        let values = values.extract::<Vec<bool>>()?;
        if ids.len() != values.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "ids and values must have same length",
            ));
        }
        self.with_inner(|inner| {
            for (id, v) in ids.iter().zip(values.iter()) {
                inner.queue_raw_write(
                    *id,
                    SmblmbRegister::TorqueEnable as u16,
                    vec![u16::from(*v)],
                );
            }
            Ok(())
        })
    }

    /// Queues an arbitrary holding-register write. Non-blocking.
    pub fn write_raw_data(
        &self,
        id: u8,
        addr: u16,
        data: &Bound<'_, pyo3::types::PyList>,
    ) -> PyResult<()> {
        let data = data.extract::<Vec<u16>>()?;
        self.with_inner(|inner| {
            inner.queue_raw_write(id, addr, data);
            Ok(())
        })
    }

    /// Blocking raw read serialized through the worker. Use sparingly;
    /// hot-loop reads should go through sync_read_present_position.
    pub fn read_raw_data(
        &self,
        py: Python,
        id: u8,
        addr: u16,
        length: u16,
    ) -> PyResult<Py<pyo3::types::PyList>> {
        let values = self.with_inner(|inner| {
            inner
                .blocking_raw_read(id, addr, length, None)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })?;
        Ok(pyo3::types::PyList::new(py, values)?.unbind())
    }

    pub fn write_goal_velocity(&self, id: u8, value: u16) -> PyResult<()> {
        self.with_inner(|inner| {
            inner.queue_raw_write(id, SmblmbRegister::GoalVelocity as u16, vec![value]);
            Ok(())
        })
    }

    pub fn write_goal_acceleration(&self, id: u8, value: u16) -> PyResult<()> {
        self.with_inner(|inner| {
            inner.queue_raw_write(id, SmblmbRegister::GoalAcceleration as u16, vec![value]);
            Ok(())
        })
    }

    pub fn write_max_torque_limit(&self, id: u8, value: u16) -> PyResult<()> {
        self.with_inner(|inner| {
            inner.queue_raw_write(id, SmblmbRegister::MaxTorqueLimit as u16, vec![value]);
            Ok(())
        })
    }

    /// Returns the number of completed worker cycles since startup.
    pub fn worker_cycles(&self) -> PyResult<u64> {
        self.with_inner(|inner| Ok(inner.cycles()))
    }

    /// Returns the duration (s) of the last completed worker cycle.
    pub fn worker_last_cycle_dt_s(&self) -> PyResult<f64> {
        self.with_inner(|inner| Ok(inner.last_cycle_dt_s()))
    }

    /// Returns and clears the last bus error message, if any.
    pub fn take_last_error(&self) -> PyResult<Option<String>> {
        self.with_inner(|inner| Ok(inner.take_last_error()))
    }

    /// Stops the worker thread and releases the serial port.
    pub fn close(&self) -> PyResult<()> {
        let mut guard = self.0.lock().unwrap();
        if let Some(mut inner) = guard.take() {
            inner.stop();
        }
        Ok(())
    }
}
