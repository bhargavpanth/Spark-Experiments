# Spark experiments

Running a few experiments and learning Spark

To run this project, make sure that you have Spark installed on your machine

## To install Spark on Mac
* Make sure you have homebrew installed
* Install Apache Spark `brew install apache-spark`
* After installation of `apache-spark` make sure that you go into the directory where its installed (`cd /usr/local/Cellar/apache-spark/<version-number>/libexec/conf`) and execute `cp log4j.properties.template log4j.properties`
* Optional: To remove some noisy logs edit the `log4j.properties` in this folder and change the log level from `INFO` to `ERROR`
* Install Python3 (Since we are using Python scripts for our experiments)
* Install pyspark `pip3 install pyspark`
* Install JDK
* Set the required paths in your bash profile (`~/.zshrc` or `~/.bashrc`)
* Add `SPARK_HOME=/usr/local/Cellar/apache-spark/<version-number>`
* Add `export PATH=$SPARK_HOME/bin:$PATH`
* Add `export PYSPARK_PYTHON=python3`


Once you do, paste the following commands in your terminal. The `ratings_counter.py` script generates a histogram of the ratings of all the movies in the dataset.

```bash
$ spark-submit ratings_counter.py
```
