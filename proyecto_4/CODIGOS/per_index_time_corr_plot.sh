#!/bin/bash

INPUT_FOLDER="/home/santiago/Escritorio/UNIVERSIDAD/FISICA_DEL_CLIMA/F-sica-del-Clima/proyecto_4/DATOS"
OUTPUT_FOLDER="/home/santiago/Escritorio/UNIVERSIDAD/FISICA_DEL_CLIMA/F-sica-del-Clima/proyecto_4/DATOS"

if [ -z "$1" ]; then
    echo "Usage: $0 INDEX_NAME"
    exit 1
fi

INDEX=$1

for file in $INPUT_FOLDER/*.nc
do
    name=$(basename "$file" .nc)
    echo "Processing $name"
    echo "CMD: grads -bpc \"run mapplot_time_corr_per_index.gs $file $OUTPUT_FOLDER/${name}.png $INDEX correlacion\""
    grads -bpc "run mapplot_time_corr_per_index.gs $file $OUTPUT_FOLDER/${name}.png $INDEX correlacion"
done