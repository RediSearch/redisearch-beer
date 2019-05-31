# small script to import the openbeerdb data to
# Redis using RediSearch

import redis
import csv
from redisearch import Client, TextField, NumericField,TagField, Query, AutoCompleter, Suggestion
# import pprint

redis_connection = {
    'host': 'localhost',
    'port': 6379
}

category = 'category'
style = 'style'
beer = 'beer'
brewery = 'brewery'

filepath = './'

catfile = filepath + 'categories.csv'
stylefile = filepath + 'styles.csv'
beerfile = filepath + 'beers.csv'
breweryfile = filepath + 'breweries.csv'
brewerygeofile = filepath + 'breweries_geocode.csv'

ftbeerindex = 'beerIdx'
breweryidx = 'breweryIdx'

# function to check if index exists and if yes drop them
def clean_index(r):
    client = Client(ftbeerindex)
    client.drop_index()
    client = Client(breweryidx)
    client.drop_index()

# function to take a csv file and import each line
# as a redis hash
def import_csv(r, keyprefix, importfile):
    header = []
    with open(importfile) as csvfile:
        reader = csv.reader(csvfile)
        keyname = ''
        header = ''
        for row in reader:

            if reader.line_num == 1:
                # column headers in the first line of the csv file.
                # we will use these for the field names in the redis hash
                header = row
                continue

            for idx, field in enumerate(row):
                if idx == 0:
                    # set the key name using the first column (id)
                    keyname = "{}:{}".format(keyprefix, field)
                    continue

                # hset each subsequent column as a field in the hash
                r.hset(keyname, header[idx], field)

# function to create the brewey redisearch index
# and add each brewery location info as a document in the index
def import_brewery_geo(r):

    # FT.CREATE
    ftcreatecmd = [
        'FT.CREATE', breweryidx, 'SCHEMA',
        'name', 'TEXT', 'WEIGHT', '5.0',
        'address', 'TEXT',
        'city', 'TEXT',
        'state', 'TEXT',
        'country', 'TEXT',
        'location', 'GEO'
    ]
    r.execute_command(*ftcreatecmd)

    with open(brewerygeofile) as geofile:
        geo = csv.reader(geofile)
        for row in geo:
            if geo.line_num == 1:
                # skip the header line
                continue

            # use the brewery id to generate the brewery key added earlier
            brewery_key = "{}:{}".format(brewery, row[1])

            # geo_key = "{}:{}".format('brewerygeo', row[1])
            # r.geoadd(geo_key, row[3], row[2])

            # get all the data from the brewery hash
            binfo = r.hgetall(brewery_key)
            # pprint.pprint(binfo)
            if not (any(binfo)):
                print ("\tERROR: Missing info for {}, skipping geo import".format(brewery_key))
                continue

            # FT.ADD the brewery location data to the redisearch brewery index
            ftaddcmd = [
                'FT.ADD', breweryidx, "ftbrewery:{}".format(row[1]),
                '1.0', 'FIELDS',
                'name', binfo[b'name'],
                'address', binfo[b'address1'],
                'city', binfo[b'city'],
                'state', binfo[b'state'],
                'country', binfo[b'country'],
                'location', row[3] + ' ' + row[2]
            ]
            try:
                r.execute_command(*ftaddcmd)
            except Exception as e:
                print ("\tERROR: Failed to add geo for {}: {}".format(brewery_key, e))
                continue

# function to create the beer redisearch index
# and add each beer as a document to the index
def ftadd_beers(r):

    # FT.CREATE
    ftcreatecmd = [
        'FT.CREATE', ftbeerindex, 'SCHEMA',
        'name', 'TEXT', 'WEIGHT', '5.0',
        'brewery', 'TEXT',
        'category', 'TAG',
        'style', 'TEXT',
        'description', 'TEXT',
        'abv', 'NUMERIC', 'SORTABLE',
        'ibu', 'NUMERIC', 'SORTABLE'
    ]
    r.execute_command(*ftcreatecmd)

    header = []
    dontadd = 0
    with open(beerfile) as csvfile:
        beers = csv.reader(csvfile)
        for row in beers:
            # we will be generating the full FT.ADD command as a list
            # then pass the whole list to redis.execute_command()
            ftaddcmd = ['FT.ADD', ftbeerindex]

            if beers.line_num == 1:
                header = row
                continue

            for idx, field in enumerate(row):
                if idx == 0:
                    ftaddcmd.append("{}:{}".format(beer, field))
                    ftaddcmd.extend(["1.0", "FIELDS"])
                    continue

                # idx 1 is brewery name
                if idx == 1:

                    if field == "":
                        # something is wrong with the csv, skip this line.
                        print ("\tEJECTING: {}".format(row))
                        dontadd = 1
                        break
                    bkey = "{}:{}".format(brewery, field)
                    ftaddcmd.extend(['brewery', r.hget(bkey, 'name')])

                # idx 2 is beer name
                elif idx == 2:

                    ftaddcmd.extend(['name', field])

                # idx 3 is category ID
                elif idx == 3:

                    catname = 'None'
                    if int(field) != -1:
                        # get the category key and hget the name of the category
                        ckey = "{}:{}".format(category, field)
                        catname = r.hget(ckey, 'cat_name')

                    ftaddcmd.extend(['category', catname])

                # idx 4 is style ID
                elif idx == 4:

                    stylename = 'None'

                    if int(field) != -1:
                        skey = "{}:{}".format(style, field)
                        stylename = r.hget(skey, 'style_name')
                    ftaddcmd.extend(['style', stylename])

                # idx 5 is ABV
                elif idx == 5:

                    ftaddcmd.extend(['abv', field])

                # idx 6 is IBU
                elif idx == 6:

                    ftaddcmd.extend(['ibu', field])

            if dontadd:
                dontadd = 0
                continue

            # FT.ADD
            r.execute_command(*ftaddcmd)

def main():

    r = redis.StrictRedis(**redis_connection)
    print("drop index")
    clean_index(r)
    print ("Importing categories...")
    import_csv(r, category, catfile)
    print ("Importing styles...")
    import_csv(r, style, stylefile)
    print ("Importing breweries...")
    import_csv(r, brewery, breweryfile)
    print ("Adding brewery geo data to RediSearch...")
    import_brewery_geo(r)
    print ("Adding beer data to RediSearch...")
    ftadd_beers(r)
    print ("Done.")

if __name__=="__main__":
    main()
