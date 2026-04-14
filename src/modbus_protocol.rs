use std::io::{Error as IoError, ErrorKind};
use std::time::Duration;

use tokio_modbus::client::sync::{rtu, Context, Reader, Writer};
use tokio_modbus::{Error as ModbusError, ExceptionCode};
use tokio_modbus::slave::SlaveContext;
use tokio_modbus::Slave;

use crate::Result;

/// Raw Modbus RTU communication handler.
pub struct ModbusProtocolHandler {
    ctx: Context,
}

impl ModbusProtocolHandler {
    /// Connect to a Modbus RTU bus.
    pub fn rtu(serial_port: &str, baudrate: u32, timeout: Duration) -> Result<Self> {
        let builder = tokio_serial::new(serial_port, baudrate);
        let mut ctx = rtu::connect_with_timeout(&builder, Some(timeout))?;
        ctx.set_timeout(Some(timeout));
        Ok(Self { ctx })
    }

    pub fn read_holding_registers(
        &mut self,
        slave_id: u8,
        addr: u16,
        quantity: u16,
    ) -> Result<Vec<u16>> {
        validate_slave_id(slave_id)?;
        if quantity == 0 {
            return Err(Box::new(IoError::new(
                ErrorKind::InvalidInput,
                "quantity must be > 0",
            )));
        }
        self.ctx.set_slave(Slave(slave_id));
        flatten_modbus_result(self.ctx.read_holding_registers(addr, quantity))
    }

    pub fn write_single_register(&mut self, slave_id: u8, addr: u16, value: u16) -> Result<()> {
        validate_slave_id(slave_id)?;
        self.ctx.set_slave(Slave(slave_id));
        flatten_modbus_result(self.ctx.write_single_register(addr, value))
    }

    pub fn write_multiple_registers(
        &mut self,
        slave_id: u8,
        addr: u16,
        values: &[u16],
    ) -> Result<()> {
        validate_slave_id(slave_id)?;
        if values.is_empty() {
            return Err(Box::new(IoError::new(
                ErrorKind::InvalidInput,
                "values must not be empty",
            )));
        }
        self.ctx.set_slave(Slave(slave_id));
        flatten_modbus_result(self.ctx.write_multiple_registers(addr, values))
    }

    pub fn read(&mut self, slave_id: u8, addr: u16, quantity: u16) -> Result<Vec<u16>> {
        self.read_holding_registers(slave_id, addr, quantity)
    }

    pub fn write(&mut self, slave_id: u8, addr: u16, data: &[u16]) -> Result<()> {
        if data.len() == 1 {
            self.write_single_register(slave_id, addr, data[0])
        } else {
            self.write_multiple_registers(slave_id, addr, data)
        }
    }

    /// Emulates sync-read semantics by polling each id sequentially.
    pub fn sync_read(&mut self, ids: &[u8], addr: u16, quantity: u16) -> Result<Vec<Vec<u16>>> {
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            out.push(self.read(id, addr, quantity)?);
        }
        Ok(out)
    }

    /// Emulates sync-write semantics by writing each id sequentially.
    pub fn sync_write(&mut self, ids: &[u8], addr: u16, data: &[Vec<u16>]) -> Result<()> {
        if ids.len() != data.len() {
            return Err(Box::new(IoError::new(
                ErrorKind::InvalidInput,
                "ids and data must have same length",
            )));
        }

        for (&id, values) in ids.iter().zip(data.iter()) {
            self.write(id, addr, values)?;
        }
        Ok(())
    }
}

fn validate_slave_id(slave_id: u8) -> Result<()> {
    if (1..=247).contains(&slave_id) {
        return Ok(());
    }
    Err(Box::new(IoError::new(
        ErrorKind::InvalidInput,
        format!("invalid Modbus slave id: {slave_id}; expected 1..=247"),
    )))
}

fn flatten_modbus_result<T>(
    res: std::result::Result<std::result::Result<T, ExceptionCode>, ModbusError>,
) -> Result<T> {
    let value = res.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    value.map_err(|exc| {
        Box::new(IoError::new(
            ErrorKind::Other,
            format!("modbus exception response: {exc:?}"),
        )) as Box<dyn std::error::Error>
    })
}
