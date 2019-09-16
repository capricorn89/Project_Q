# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""


from telegram.ext import Updater, CommandHandler
import requests
import re
from urllib.request import urlopen

api_key = '969501539:AAE9y0VBLFVF-O2DGXwPNcIUMiirE17lBgg'
contents = requests.get('https://random.dog/woof.json').json()
image_url = contents['url']

def get_url():
    contents = requests.get('https://random.dog/woof.json').json()    
    url = contents['url']
    return url

# 여러 사진 하나씩
def bop(bot, update):
    urls = [get_url() for i in range(5)]
    for url in urls:
        ret = urlopen(url)
        if ret.code == 200:
            chat_id = update.message.chat_id
            bot.sendPhoto(chat_id=chat_id, photo=url)

# 여러 사진 하나씩
def bops(bot, update):
    urls = [get_url() for i in range(2)]
    chat_id = update.message.chat_id
    print(urls[0])
    print(urls[1])
    bot.sendMediaGroup(chat_id = chat_id, 
                       media = [{'type' : 'photo', 'media' : urls[0]}, {'type' : 'photo', 'media' : urls[1]}])
    print(urls[0])
    print(urls[1])
    
updater = Updater(api_key)
dp = updater.dispatcher
dp.add_handler(CommandHandler('bops',bops))
dp.add_handler(CommandHandler('bop',bop))

updater.start_polling()
updater.idle()
    

