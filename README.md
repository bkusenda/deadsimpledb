# Dead Simple DB (DSDB)
Dead Simple Database is a key-value database focused on storing data to the file system in a standard readable format (eg JSON, pickle, CSV, jpg, png, numpy etc).  It's more appopriate to describe describe it as a simplified interface to the filesystem.  This library may be useful for those who want to read the data directly from the file system without the need for an intermediate data access layer/library.

Version: 0.1-alpha

## Features
- Stores entries in standard format
- Small code base - only one python file
- Easily customizable - feel free to create different writers, S3, WebDav, etc

## When to use DSDB?
- You want a simple interface for writing data to the file system

## Limitations
- No indexes
- No threading or Multi processing support

## Usage
```python
db = DeadSimpleDB("testdb")

db.save(['entity',1],value={'value':1000})
db.save(['entity',2],value={'value':2000})
db.save(['entity',3],value={'value':3000}.flush=True,clear_cache=True)

db.get(['entity',3])

db.save(['stats',1],value=np.random.rand(3,3), stype='pkl')
db.flush_all()

```

## Requirements

- simplejson
- pickle
