#!/bin/bash

cd `dirname $0`

./forall ./blocknotify2.sh --host 127.0.0.1 $*
