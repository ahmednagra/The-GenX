import os
import scrapy
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem  # Correctly import DropItem

class ProductImagesPipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        # Extract product_images from the item
        product_images = item.get('product_images', [])
        for image_url in product_images:
            yield scrapy.Request(url=image_url, meta={'item': item})

    def file_path(self, request, response=None, info=None, *, item=None):
        # Extract the filename directly from the request URL
        return request.url.split('/')[-1]

    def item_completed(self, results, item, info):
        # Collect the paths of downloaded images
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem("No images downloaded")  # Raise the DropItem exception if no images were downloaded
        item['image_paths'] = image_paths
        return item
