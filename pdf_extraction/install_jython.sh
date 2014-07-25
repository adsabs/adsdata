#!/bin/sh

wget -O jython-installer.jar "http://search.maven.org/remotecontent?filepath=org/python/jython-installer/2.7-b2/jython-installer-2.7-b2.jar"
java -jar jython-installer.jar --console
