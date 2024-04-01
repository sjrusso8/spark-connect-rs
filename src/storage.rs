//! Enum for handling Spark Storage representations

use crate::spark;

#[derive(Clone, Copy, Debug)]
pub enum StorageLevel {
    None,
    DiskOnly,
    DiskOnly2,
    DiskOnly3,
    MemoryOnly,
    MemoryOnly2,
    MemoryAndDisk,
    MemoryAndDisk2,
    OffHeap,
    MemoryAndDiskDeser,
}

impl From<spark::StorageLevel> for StorageLevel {
    fn from(spark_level: spark::StorageLevel) -> Self {
        match (
            spark_level.use_disk,
            spark_level.use_memory,
            spark_level.use_off_heap,
            spark_level.deserialized,
            spark_level.replication,
        ) {
            (false, false, false, false, _) => StorageLevel::None,
            (true, false, false, false, 1) => StorageLevel::DiskOnly,
            (true, false, false, false, 2) => StorageLevel::DiskOnly2,
            (true, false, false, false, 3) => StorageLevel::DiskOnly3,
            (false, true, false, false, 1) => StorageLevel::MemoryOnly,
            (false, true, false, false, 2) => StorageLevel::MemoryOnly2,
            (true, true, false, false, 1) => StorageLevel::MemoryAndDisk,
            (true, true, false, false, 2) => StorageLevel::MemoryAndDisk2,
            (true, true, true, false, 1) => StorageLevel::OffHeap,
            (true, true, false, true, 1) => StorageLevel::MemoryAndDiskDeser,
            _ => unimplemented!(),
        }
    }
}

impl From<StorageLevel> for spark::StorageLevel {
    fn from(storage: StorageLevel) -> spark::StorageLevel {
        match storage {
            StorageLevel::None => spark::StorageLevel {
                use_disk: false,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            },
            StorageLevel::DiskOnly => spark::StorageLevel {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            },
            StorageLevel::DiskOnly2 => spark::StorageLevel {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            },
            StorageLevel::DiskOnly3 => spark::StorageLevel {
                use_disk: true,
                use_memory: false,
                use_off_heap: false,
                deserialized: false,
                replication: 3,
            },
            StorageLevel::MemoryOnly => spark::StorageLevel {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            },
            StorageLevel::MemoryOnly2 => spark::StorageLevel {
                use_disk: false,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            },
            StorageLevel::MemoryAndDisk => spark::StorageLevel {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 1,
            },
            StorageLevel::MemoryAndDisk2 => spark::StorageLevel {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: false,
                replication: 2,
            },
            StorageLevel::OffHeap => spark::StorageLevel {
                use_disk: true,
                use_memory: true,
                use_off_heap: true,
                deserialized: false,
                replication: 1,
            },
            StorageLevel::MemoryAndDiskDeser => spark::StorageLevel {
                use_disk: true,
                use_memory: true,
                use_off_heap: false,
                deserialized: true,
                replication: 1,
            },
        }
    }
}
