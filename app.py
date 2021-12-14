import asyncio
import threading
from datetime import datetime
from typing import Optional

import fiona
import geopandas as gpd
import requests
from flask import Flask, make_response

NASA_LINK = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/"

DATA_LINKS = [
    f"{NASA_LINK}/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Global_24h.zip",
    f"{NASA_LINK}/modis-c6.1/shapes/zips/MODIS_C6_1_Global_24h.zip",
    f"{NASA_LINK}/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip",
]


app = Flask(__name__)
app.config["current_data"] = (None, 0)


def update_nasa_data() -> None:
    gpd_data: Optional[gpd.GeoDataFrame] = None
    for link in DATA_LINKS:
        r = requests.get(link)
        if r.status_code == 200:
            with fiona.BytesCollection(r.content) as b:
                if gpd_data is None:
                    gpd_data = gpd.GeoDataFrame.from_features(b)
                else:
                    gpd_data = gpd_data.append(
                        gpd.GeoDataFrame.from_features(b)
                    )
    app.config["current_data"] = gpd_data


@asyncio.coroutine
def schedule():
    while True:
        current_dt = datetime.now()
        if current_dt.hour == 3 and current_dt.minute > 30:
            update_nasa_data()
        yield from asyncio.sleep(1800)


def wrapper(run_loop):
    asyncio.set_event_loop(run_loop)
    loop.run_until_complete(schedule())


@app.route("/")
def health():
    return make_response("Ok", 200)


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    t = threading.Thread(target=wrapper, args=(loop,))
    t.start()
    update_nasa_data()
    app.run()
