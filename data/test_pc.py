import requests
from lxml import etree
url = "https://deyang.zbj.com/search/f/?kw=%E7%BD%91%E7%AB%99%E5%BB%BA%E8%AE%BE&#34;"
resp = requests.get(url)
#print(resp.text)

html = etree.HTML(resp.text)
# 拿到每个小信息
divs = html.xpath("/html/body/div[6]/div/div/div[3]/div[5]/div[1]/div[*]")
for div in divs:
    price = div.xpath("./div/div/a[2]/div[2]/div[1]/span[1]/text()")[0].strip("¥")
    print(price)