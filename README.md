# spark-playground
Playground from Spark, with some helpful Python 3.5 scripts

## Configure
    # Install JDK 8 and then
    wget http://ftp.unicamp.br/pub/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
    tar -xvf spark-2.2.1-bin-hadoop2.7.tgz
    echo "export SPARK_HOME=/your/spark/directory/spark-2.2.1-bin-hadoop2.7" >> ~/.bash_profile
    echo 'export PATH="\${SPARK_HOME}/bin:\${PATH}"' >> ~/.bash_profile

## HowTo

    spark-submit ratings-counter.py
    spark-submit friends-by-age.py