```shell script
scrapy startproject tutorial

```

```shell script
scrapy crawl quotes
```

```shell script
scrapy shell "http://quotes.toscrape.com/page/1/"

```

* Spider ```vietstockpricesboard_minute``` runs every minute
* Spider ```vietstockpricesboard_hour``` runs every hour
* Spider ```vietstockpricesboard``` runs every day
* Spider ```vietstockfinanceinfo``` run every day (to update Finance Info)
* Spider ```vietstockaz``` run every day (to update company info)