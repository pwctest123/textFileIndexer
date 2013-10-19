#!/bin/bash

rm -rf text.zip

zip text.zip textFileIndexer-*-shaded.jar src/main/resources/azkaban/*.job

