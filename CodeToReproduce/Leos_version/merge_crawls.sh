echo "merging crawls"

export YEAR=2021
export DB_DIR="outputs/"

python merge_crawls.py --year $YEAR --database_dir $DB_DIR