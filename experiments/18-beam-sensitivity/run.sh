#! /bin/bash

beam=$1
recrun=$(realpath fpqb_run.pkl)

pushd ../../

brad admin replay_planner \
  --schema-name imdb_extended_100g \
  --recorded-run $recrun \
  --beam-size $beam
