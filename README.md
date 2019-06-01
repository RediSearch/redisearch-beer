# rediseach-beer

use the https://openbeerdb.com/ csv files to import data to Redis and some RediSearch indicies

To import the data, start a Redis and run [import.py](../master/import.py) with python3

The csv files are available on the openbeerdb.com site, but I had to change the [beers.csv](../master/beers.csv) file because it was malformed. That's why I am making the csv files available here.

After the data is imported, you can query it using RediSearch. Some example commands:

Irish Ale and German Ale beers with ABV greater than 9%:
`ft.search beerIdx "@category:{Irish Ale|German Ale} @abv:[9 inf]"`

All beers with ABV higher than 5% but lower than 6%:
`ft.search beerIdx @abv:"[5 6]"`

Breweies in a 10km radius from the coordinates of Chicago, IL USA:
`ft.search breweryIdx "@location:[-87.623177 41.881832 10 km]"`

The beers are added to the RediSearch index weighted by ABV. So by default, the results will be ordered by ABV highest to lowest. Both ABV and IBU are sortable, so you can order results by either of these fields using `sortby` in the query.