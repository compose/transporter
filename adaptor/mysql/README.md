# MySQL adaptor


## Setup and testing on MacOS with Pkgsrc (other package managers are available)

1. Install client and server

		sudo pkgin install mysql-client
		sudo pkgin install mysql-server

2. Edit `/opt/pkg/etc/my.cnf` and point `data-dir` somewhere (I opted 
for `~/Database/mysql`). Add `secure_file_priv = "/tmp"` too.

3. Run `mysql_install_db`

4. Run it `cd /opt/pkg ; /opt/pkg/bin/mysqld_safe &`


## Development notes

This is being built using the Postgresql adaptor as a basis and using
[go-sql-driver/mysql](https://github.com/go-sql-driver/mysql). It's noted that
[go-mysql-org](https://github.com/go-mysql-org) and in particular
[canal](https://github.com/go-mysql-org/go-mysql#canal) look like a good
alternative though. 

### Element types

Postgresql has an ARRAY data type so for each array also pulls the [element
type](https://www.postgresql.org/docs/9.6/infoschema-element-types.html) within

> When a table column [...] the respective information schema view only contains
> ARRAY in the column data_type.

This happens under the `iterateTable` function. Note that here the `c` is a sql
variable; Not to be confused with the `c` variable outside of this; Yay for
naming. If we want to run these queries manually the only bits that change are
the `%v`. E.g: `...WHERE c.table_schema = 'public' AND c.table_name = 't_random'`.
The query will output something like this: 

		 column_name | data_type | element_type 
		-------------+-----------+--------------
		s           | integer   | 
		md5         | text      | 
		(2 rows)
		
### Data types

Comparing differences from Postgresql using these sources:

- <https://www.postgresql.org/docs/9.6/datatype.html>
- <https://dev.mysql.com/doc/refman/5.7/en/data-types.html>

There are three code areas that need changing:

1. `colXXX` constants at top of adaptor\_test.go
2. `setupData` in adaptor\_test.go
3. `TestReadComplex` in reader\_test.go

Some comments:

- No ARRAY in MySQL
- [Timestamp assumes UTC](https://dev.mysql.com/doc/refman/8.0/en/datetime.html)
- The `--colbytea` bits are all just comments so it's easier to match things up
- On that note I'm re-ording things so it's consistent
- [Inserting binary can be done like this](https://stackoverflow.com/a/10283197/208793)
- No BIGSERIAL, etc
- Geometry is there, just a bit different
- No CIDR
- ENUM has to be done a bit differently, no need to CREATE TYPE

I'm currently developing with a ye-olde 5.6 version so it doesn't like:

- ENUM
- SET 
- VARBINARY
- JSON

### TestReadComplex Notes

#### Text

Remove newline for now for `text`:

```
--- FAIL: TestReadComplex (0.01s)
reader_test.go:117: Expected coltext of row to equal this is \n extremely important (string), but was this is 
	 extremely important (string)
```

#### Float

> Float MySQL also supports this optional precision specification, but the
> precision value in FLOAT(p) is used only to determine storage size. A precision
> from 0 to 23 results in a 4-byte single-precision FLOAT column. A precision from
> 24 to 53 results in an 8-byte double-precision DOUBLE column.

#### Blob

Tried using `go:embded` and inserting the blob data as a string, but couldn't
get it to work. I.e.

```
// For testing Blob
//go:embed logo-mysql-170x115.png
blobdata string
```

And then:

```
fmt.Sprintf(`INSERT INTO %s VALUES (... '%s');`, blobdata)
```

Tried with `'%s'` (didn't work at all) and `%q` which inserted, but didn't
extract correctly.

In the end I used `LOAD_FILE` to insert (like you are probably meant to), but
would be nice to do directly from Go.

Ultimately I'll probably remove this test.

#### Spatial

This is a handy package: https://github.com/paulmach/orb

Need to think about how we want to handle spatial types:

1. Decode from WKB in reader.go before we get to testing OR
2. Leave as WKB, decode for the test only OR
3. Leave as WKB, don't decode at all, instead encode the test data to match

Another good option: https://github.com/twpayne/go-geom

Struggling. I think I'd like to take the "raw" data and decode for the test.

Another option: https://github.com/paulsmith/gogeos

From here: https://dev.mysql.com/doc/refman/5.6/en/gis-data-formats.html

> Internally, MySQL stores geometry values in a format that is not identical to
> either WKT or WKB format. (Internal format is like WKB but with an initial 4
> bytes to indicate the SRID.)

> For the WKB part, these MySQL-specific considerations apply:
>
> - The byte-order indicator byte is 1 because MySQL stores geometries as little-endian values.
>
> - MySQL supports geometry types of Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection. Other geometry types are not supported.

Maybe we should just strip the SRID? Then we'd be left with just wkb

Getting ahead a bit, but need to think about how we transfer things MySQL to
MySQL and MySQL to X.

I managed to get things working with go-geom and reading the MySQL data as hex.
go-geom has handy wkbhex functions that Orb doesn't. It's _possible_ we fell
foul of this with Orb:

> Scanning directly from MySQL columns is supported. By default MySQL returns
> geometry data as WKB but prefixed with a 4 byte SRID. To support this, if the
> data is not valid WKB, the code will strip the first 4 bytes, the SRID, and try
> again. **This works for most use cases**. 

Emphasis mine.

I've had to strip off the SRID to get things to work with go-geom. Going to Hex
allows us to do that.

#### Bit

TODO (write words here)

#### Binary

This is probably very similar to Blob. At the moment we store a Hex value and
for the purposes of testing and comparison we then convert that to a string
representation of the hex value on read. 