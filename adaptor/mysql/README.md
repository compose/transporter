# MySQL adaptor


## Setup and testing on MacOS with Pkgsrc (other package managers are available)

1. Install client and server

		sudo pkgin install mysql-client
		sudo pkgin install mysql-server

2. Edit `/opt/pkg/etc/my.cnf` and point `data-dir` somewhere (I opted for `~/Database/mysql`)

3. Run `mysql_install_db`

4. Run it `cd /opt/pkg ; /opt/pkg/bin/mysqld_safe &`