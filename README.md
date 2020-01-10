## Overview

This is implemented code for SurfStore project.
For detailed overview, please see https://cseweb.ucsd.edu/~gmporter/classes/sp18/cse124/post/project2/


## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean
