mkdir $1
cp topology.yaml ./$1/
storm jar ../target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar srg.LinTopology ./$1/topology.yaml $1
