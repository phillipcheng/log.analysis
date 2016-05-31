#!/bin/bash

LA_HOME=/data/log.analysis
PDE_HOME=/data/pde/target

export LA_EXT=$PDE_HOME/pde-0.0.1.jar

#$1 is output, $2 is input
$LA_HOME/bin/preload.sh preload.pde.csv.properties $1 nothing.evt $2