from rmtest import BaseModuleTestCase
import redis
import unittest

r = redis.Redis(host='localhost', port=6379, db=0)

class SearchBeerTest(unittest.TestCase):

    def testGeneral(self):
        """Test """
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.client_setname(self._testMethodName)
        
        res = r.execute_command('FT.SEARCH', 'beerIdx', '@category:Irish Ale|German Ale @abv:[9 inf]')
        self.assertEquals(len(res), 7)
        
        res = r.execute_command('FT.SEARCH', 'beerIdx', '@abv:[5 6]')
        self.assertEquals(len(res), 21)

        res = r.execute_command('FT.SEARCH', 'breweryIdx', '@location:[-87.623177 41.881832 10 km]')
        self.assertEquals(len(res), 17)


if __name__ == '__main__':
    unittest.main()
