# MySQL adaptor


## Setup and testing on MacOS with Pkgsrc (other package managers are available)

1. Install client and server

		sudo pkgin install mysql-client
		sudo pkgin install mysql-server

2. Edit `/opt/pkg/etc/my.cnf` and point `data-dir` somewhere (I opted for `~/Database/mysql`)

3. Run `mysql_install_db`

4. Run it `cd /opt/pkg ; /opt/pkg/bin/mysqld_safe &`


## Development notes

This is being built using the Postgresql adaptor as a basis and using [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql). It's noted that [go-mysql-org](https://github.com/go-mysql-org) and in particular [canal](https://github.com/go-mysql-org/go-mysql#canal) look like a good alternative though.

### Element types

Postgresql has an ARRAY data type so for each array also pulls the [element type](https://www.postgresql.org/docs/9.6/infoschema-element-types.html) within

> When a table column [...] the respective information schema view only contains ARRAY in the column data_type.

This happens under the `iterateTable` function. Note that here the `c` is a sql variable; Not to be confused with the `c` variable outside of this; Yay for naming. If we want to run these queries manually the only bits that change are the `%v`. E.g: `...WHERE c.table_schema = 'public' AND c.table_name = 't_random'`. The query will output something like this:

		 column_name | data_type | element_type 
		-------------+-----------+--------------
		s           | integer   | 
		md5         | text      | 
		(2 rows)
