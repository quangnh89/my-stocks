```shell script
scrapy startproject tutorial

```

```shell script
scrapy crawl quotes
```

```shell script
scrapy shell "http://quotes.toscrape.com/page/1/"

```

* Spider parameters
```shell script
scrapy crawl myspider -a category=electronics
```

```python
import scrapy

class MySpider(scrapy.Spider):
    name = 'myspider'

    def __init__(self, category=None, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.start_urls = [f'http://www.example.com/categories/{category}']
        # ...

```

* Spider ```vietstockpricesboard``` runs every day
```shell script
scrapy crawl vietstockpricesboard -a resolution=1 # for minute
scrapy crawl vietstockpricesboard -a resolution=60 # for for hour
scrapy crawl vietstockpricesboard -a resolution=D # for day
```
* Spider ```vietstockfinanceinfo``` run every day (to update Finance Info)
* Spider ```vietstockaz``` run every day (to update company info)