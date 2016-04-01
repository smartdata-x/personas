# personas
[![Build Status](https://travis-ci.org/smartdata-x/personas.svg?branch=master)](https://travis-ci.org/smartdata-x/personas)
## 用户画像 ##
1. 过滤出访问了特定网站的AD和User-Agent的URL
2. 通过URL提取出用户的QQ等信息并存储
3. 过滤出符合爬虫客户端爬取数据的AD,UA,URL和Cookie给爬虫

## 用户个人信息 ##

1. 从cookies里获取用户QQ，手机，微博号，邮箱(qq邮箱)
2. QQ号，邮箱通过qq.com获取，获取的QQ为某个AD登录过的所有QQ
3. 手机通过suning.com获取
4. 微博号通过weibo.com获取
