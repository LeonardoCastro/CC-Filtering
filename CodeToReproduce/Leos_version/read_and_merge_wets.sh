echo "running read_wets.py"

export YEAR=2023
export WET_DIR="wet_paths/"

python read_wets.py --year $YEAR --wet_paths_dir $WET_DIR


echo "running merge_crawls.py"

export DB_DIR="outputs/"

python merge_crawls.py --year $YEAR --database_dir $DB_DIR
