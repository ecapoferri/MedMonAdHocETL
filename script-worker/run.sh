#!/bin/bash
python $APP_PATH && psql --file=$APP_PATH/cascade_tables.sql $CONN_STRING