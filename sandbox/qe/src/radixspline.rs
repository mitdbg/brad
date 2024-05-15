#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use arrow::{array::{UInt64Array, Array}, record_batch::RecordBatch};
use std::os::raw::c_void;

// pub use build;
// pub use lookup;
// pub use clear;

pub struct RadixSpline {
    rs_ptr: *mut c_void,
}

impl RadixSpline {

    pub fn build_simple(data: [u64; 15]) -> RadixSpline {
        unsafe {
            let rs_ptr = build(data.as_ptr(), 15);
            RadixSpline {
                rs_ptr,
            }
        }
    }

    pub fn build(record_batch: &RecordBatch, column_index: usize) -> RadixSpline {
        let column = record_batch.column(column_index);
        let u64_array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
        
        let ptr = u64_array.values().as_ptr();
        let size = column.len() as u64;
        unsafe {
            let rs_ptr = build(ptr, size);
            RadixSpline {
                rs_ptr,
            }
        }
    }

    pub fn lookup(&self, key: u64) -> bool {
        unsafe {
            lookup(self.rs_ptr, key)
        }
    }

    pub fn clear(&self) {
        unsafe {
            clear(self.rs_ptr)
        }
    }
}