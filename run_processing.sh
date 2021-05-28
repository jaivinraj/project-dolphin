while getopts s: flag
do
    case "${flag}" in
        s) searchname=${OPTARG};;
    esac
done

docker exec -it dolphin-server python pipeline/01_create_tables.py --searchname $searchname || exit 1
docker exec -it dolphin-scraper python pipeline/02_scrape_data.py --searchname $searchname || exit 1
docker exec -it dolphin-server python pipeline/03_get_new_addresses.py --searchname $searchname || exit 1
docker exec -it dolphin-server python pipeline/04_geocode_addresses.py --searchname $searchname || exit 1
docker exec -it dolphin-server python pipeline/05_convert_bng.py --searchname $searchname || exit 1
docker exec -it dolphin-server python pipeline/06_get_closest_pois.py --searchname $searchname || exit 1
docker exec -it dolphin-server python pipeline/07_snap_to_road_network.py --searchname $searchname || exit 1
docker exec -it dolphin-server python pipeline/08_get_poi_distances.py --searchname $searchname || exit 1
docker exec -it dolphin-server python pipeline/09_complete_processing.py --searchname $searchname || exit 1
