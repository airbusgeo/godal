#!/bin/bash

shopt -s globstar
tail -n 14 LICENSE > _copyright.txt
for i in **/*.cpp **/*.h **/*.go;
do
  if ! grep -q Copyright $i
  then
    cat _copyright.txt $i >$i.new && mv $i.new $i
  fi
done
rm _copyright.txt
