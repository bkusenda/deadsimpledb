# Dead Simple Database (DSDB)

Dead Simple Database is a key-value database focused on storing data to the file system in a standard readable format (eg JSON, pickle, jpg, png, etc).  It's more appopriate to describe describe it as a simplified interface to the filesystem.  This library may be useful for those who want to read the data directly from the file system without the need for an intermediate data access layer/library.

Version: 0.0.1

## Features

- Stores entries in standard/native formats
- Small code base - **just one python file**
- Light weight (easily add different file systems or serialization formats)
- Schemaless

## When to use DSDB

- You want a simple key-value database like interface for writing data to the file system

## Limitations

- No indexes (Yet)
- No threading or Multi processing support (Yet)

## Installation

```
pip install git+https://github.com/bkusenda/deadsimpledb
```

## Usage

The below configuration will store database files in the ```ddb``` directory.

```python

# Create database object
db = DeadSimpleDB(root_path="ddb")

# Add some dictionary data and use json (default storage type)
db.save(['entity',1],value={'value':1000})
db.save(['entity',2],value={'value':"Hello"})
db.save(['entity',3],value={'value':"World"}.flush=True,clear_cache=True)

# Add a numpy entry and store in pickel format
db.save(['stats',1],value=np.random.rand(3,3), stype='pkl')

# Save Entries to Disk
db.flush_all()

# retrieve an entry
stored_value = db.get(['entity',3])
```

## Requirements

- simplejson
