# Apache Zeppelin R Interpreter

This is a interperter for [R](http://cran.r-project.org) code for the [Apache Zeppelin notebook](http://zeppelin.incubator.apache.org).

[![R Interpreter Screenshot](http://datalayer.io/ext/screenshots/R-interpreter.png)](http://datalayer.io)

It support cross paragraph variables and R plot (ggplot2...).

Due to GPL2 license of used libraries, this software is not released under ASL2 ([read more](http://www.apache.org/foundation/license-faq.html#GPL)).

# Prerequisite

You need to have R (with Rserve, ggplot2 knitr) available on the host running the notebook.

For Centos: `yum install R R-devel`

For Ubuntu: `apt-get install r-base r-cran-rserve`

Launch R commands tos install the needed packages:

```
R CMD BATCH install.packages("Rserve")
R CMD BATCH install.packages("ggplot2")
R CMD BATCH install.packages("knitr")
```

# Build and Run

```
mvn install -DskipTests
./bin/zeppelin.sh
```

# Docker

If you don't want to build, get the Docker image with `docker pull datalayer/zeppelin` (it might take a while to download) and launch with `./zeppelin-docker-start`.


# Licensed under GNU General Public License

Copyright (c) 2015 Datalayer (http://datalayer.io)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

[![R](http://datalayer.io/ext/images/logo-R-200.png)](http://cran.r-project.org)

[![Apache Zeppelin](http://datalayer.io/ext/images/logo-zeppelin-small.png)](http://zeppelin.incubator.apache.org)

[![Datalayer](http://datalayer.io/ext/images/logo_horizontal_072ppi.png)](http://datalayer.io)

