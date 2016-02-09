#!/bin/bash -eu

for v in "$@"; do
  echo "Doing $v..."

  sed -i "s#mark--><version>.*</version>#mark--><version>$v</version>#" pom.xml

  mvn clean install

  java -jar target/benchmarks.jar verifyNodesHaveLabel -f 1 -wi 30 -i 30 -rf csv -rff "$v.csv"

  echo "Done with $v"
done
