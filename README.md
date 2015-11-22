# Apache Zeppelin R

This adds [R](http://cran.r-project.org) interpeter to the [Apache Zeppelin notebook](http://zeppelin.incubator.apache.org).

It supports:

+ R code.
+ SparkR code.
+ Cross paragraph R variables.
+ R plot (ggplot2...).

For Scala / R binding, please use the [rscala branch](https://github.com/datalayer/zeppelin-R/tree/rscala).

## Simple R

[![Simple R](https://raw.githubusercontent.com/datalayer/zeppelin-R/rserve/_Rimg/simple-r.png)](https://raw.githubusercontent.com/datalayer/zeppelin-R/rserve/_Rimg/simple-r.png)

## Plot

[![Plot](https://raw.githubusercontent.com/datalayer/zeppelin-R/rserve/_Rimg/plot.png)](https://raw.githubusercontent.com/datalayer/zeppelin-R/rserve/_Rimg/plot.png)

## SparkR

[![SparkR](https://raw.githubusercontent.com/datalayer/zeppelin-R/rserve/_Rimg/sparkr.png)](https://raw.githubusercontent.com/datalayer/zeppelin-R/rserve/_Rimg/sparkr.png)

# Prerequisite

You need R available on the host running the notebook.

+ For Centos: `yum install R R-devel`
+ For Ubuntu: `apt-get install r-base r-cran-rserve`

Install additional R packages:

```
R CMD BATCH install.packages("Rserve")
R CMD BATCH install.packages("ggplot2")
R CMD BATCH install.packages("knitr")
```

You also need a compiled version of Spark 1.5.0. Download [the binary distribution](http://archive.apache.org/dist/spark/spark-1.5.0/spark-1.5.0-bin-hadoop2.6.tgz) and untar to make it accessible in `/opt/spark` folder.

# Build and Run

```
mvn clean install -Pspark-1.5 -Dspark.version=1.5.0 -Dhadoop.version=2.7.1 -Phadoop-2.6 -Ppyspark -Dmaven.findbugs.enable=false -Drat.skip=true -Dcheckstyle.skip=true -DskipTests -pl '!flink,!ignite,!phoenix,!postgresql,!tajo,!hive,!cassandra,!lens,!kylin'
```

```
SPARK_HOME=/opt/spark ./bin/zeppelin.sh
```

Go to [http://localhost:8080](http://localhost:8080) and test the `R Tutorial` note.

## Get the image from the Docker Repository

For your convenience, [Datalayer](http://datalayer.io) provides an up-to-date Docker image for [Apache Zeppelin](http://zeppelin.incubator.apache.org), the WEB Notebook for Big Data Science.

In order to get the image, you can run with the appropriate rights:

`docker pull datalayer/zeppelin-rserve`

Run the Zeppelin notebook with:

`docker run -it -p 2222:22 -p 8080:8080 -p 4040:4040 datalayer/zeppelin-rserve`

and go to [http://localhost:8080](http://localhost:8080) and test the `R Tutorial` note.

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

