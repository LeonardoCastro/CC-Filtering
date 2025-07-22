# CC-filtering

A simple executor to download the different crawls for a given year from the CommonCrawl. The crawls are processed to obtain the websites with:
1. A .co.uk address
2. A British postcode

## Requirements

CC-filtering has a minimal list of requirements:
- pandas
- tqdm
- warcio

## Usage

### 1. Processing 1 wet.paths file

```bash
$ chmod +x read_wet.sh
# make sure that the right path and name of the wet file is written in the script and then:
$ ./read_wet.sh 
```

### 2. Processing 1 year of crawls

Make sure that the wet.paths are in a single directory. The recommended format is `{crawl}_wet.paths`, e.g. `202350_wet.paths`

```bash
$ chmod +x read_wets.sh
# make sure to put the right year and directory to wet.paths
$ ./read_wets.sh
```

### 3. Merging crawls

```bash
$ chmod +x merge_crawls.sh
# make sure to put the right year and directory to csvs
$ ./merge_crawls.sh
```

### 4. Processing and merging

```bash
$ chmod +x read_and_merge_wets.sh
# make sure to put the right year and directories
$ ./read_and_merge_wets.sh
```