from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader
from dataextraction import get_locations

class JobLoader(ItemLoader):

    # By default, ItemLoader will not do anything with the data it receives

    default_output_processor = TakeFirst()

    location_in = MapCompose(get_locations)
    # url_in = MapCompose(lambda x: 'https://www.mckinsey.com/careers/search-jobs' + x )